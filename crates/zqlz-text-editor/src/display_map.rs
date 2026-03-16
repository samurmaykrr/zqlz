use crate::{
    AnchoredCodeAction, AnchoredDiagnostic, AnchoredInlayHint, BufferSnapshot, FoldRegion,
    Highlight, buffer::Position, syntax::HighlightKind,
};
use gpui::Pixels;
use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DisplayPoint {
    pub row: usize,
    pub column: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowInfo {
    pub row_id: DisplayRowId,
    pub display_row: usize,
    pub buffer_line: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct VisualRowInfo {
    pub row_id: DisplayRowId,
    pub visual_row: usize,
    pub display_row: usize,
    pub buffer_line: usize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DisplayRowId {
    pub buffer_line: usize,
    pub wrap_subrow: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChunkHighlight {
    pub start: usize,
    pub end: usize,
    pub kind: HighlightKind,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DisplayTextChunk {
    pub row_id: DisplayRowId,
    pub display_row: usize,
    pub buffer_line: usize,
    pub start_offset: usize,
    pub text: String,
    pub highlights: Vec<ChunkHighlight>,
    pub diagnostics: Vec<std::ops::Range<usize>>,
    pub inlay_hints: Vec<AnchoredInlayHint>,
}

#[derive(Clone, Debug)]
pub struct DisplayViewport {
    visible_rows: Range<usize>,
    row_infos: Arc<Vec<RowInfo>>,
    text_chunks: Arc<Vec<DisplayTextChunk>>,
    block_widgets: Arc<Vec<(usize, BlockWidgetChunk)>>,
}

#[derive(Clone, Debug, Default)]
pub struct DisplayRowIndex {
    display_slots_by_buffer_line: Arc<Vec<Option<usize>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StickyHeaderChunk {
    pub buffer_line: usize,
    pub display_row: usize,
    pub text: String,
    pub kind_label: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockWidgetChunk {
    pub line: usize,
    pub label: String,
}

#[derive(Clone, Debug)]
pub struct DisplaySnapshot {
    buffer: BufferSnapshot,
    fold_snapshot: FoldSnapshot,
    tab_snapshot: TabSnapshot,
    wrap_snapshot: WrapSnapshot,
    highlight_snapshot: HighlightSnapshot,
    inlay_snapshot: InlaySnapshot,
    block_snapshot: BlockSnapshot,
    chunk_snapshot: DisplayChunkSnapshot,
}

#[derive(Clone, Debug, Default)]
pub struct DisplayMap {
    fold_map: FoldMap,
    tab_map: TabMap,
    wrap_map: WrapMap,
    highlight_map: HighlightMap,
    inlay_map: InlayMap,
    block_map: BlockMap,
}

#[derive(Clone, Debug, Default)]
pub struct FoldMap {
    display_lines: Arc<Vec<usize>>,
    row_index: DisplayRowIndex,
    last_sync_range: Option<Range<usize>>,
    total_lines: usize,
    fold_regions: Vec<FoldRegion>,
    folded_lines: HashSet<usize>,
}

#[derive(Clone, Debug, Default)]
pub struct WrapMap {
    visual_rows_by_line: Arc<Vec<usize>>,
    total_lines: usize,
    wrap_column: usize,
    last_sync_range: Option<Range<usize>>,
}

#[derive(Clone, Debug)]
pub struct TabMap {
    tab_size: usize,
}

#[derive(Clone, Debug, Default)]
pub struct HighlightMap {
    state: RefCell<HighlightMapState>,
}

#[derive(Clone, Debug, Default)]
pub struct InlayMap {
    state: RefCell<InlayMapState>,
}

#[derive(Clone, Debug, Default)]
pub struct BlockMap {
    state: RefCell<BlockMapState>,
}

#[derive(Clone, Debug, Default)]
struct BlockMapState {
    block_widgets: Arc<Vec<BlockWidgetChunk>>,
    block_indexes_by_line: Arc<Vec<Vec<usize>>>,
    total_lines: usize,
    last_sync_range: Option<Range<usize>>,
}

#[derive(Clone, Debug, Default)]
pub struct FoldDisplayState {
    folded_lines: HashSet<usize>,
    display_map: DisplayMap,
    cached_display_lines: Arc<Vec<usize>>,
}

#[derive(Clone, Debug)]
pub struct FoldSnapshot {
    display_lines: Arc<Vec<usize>>,
    row_index: DisplayRowIndex,
    folded_lines: HashSet<usize>,
    fold_regions: Vec<FoldRegion>,
}

#[derive(Clone, Debug)]
pub struct WrapSnapshot {
    soft_wrap: bool,
    display_lines: Arc<Vec<usize>>,
    visual_rows_by_line: Arc<Vec<usize>>,
    visual_row_starts_by_display_row: Arc<Vec<usize>>,
    display_rows_by_visual_row: Arc<Vec<usize>>,
    wrap_column: usize,
    layout_cache: RefCell<Option<WrapLayoutCacheEntry>>,
}

#[derive(Clone, Debug)]
pub struct TabSnapshot {
    tab_size: usize,
}

#[derive(Clone, Debug)]
pub struct VisibleWrapLayout {
    visible_rows: Range<usize>,
    line_y_offsets: Arc<Vec<Pixels>>,
    visual_rows: Arc<Vec<usize>>,
    wrap_column: usize,
}

#[derive(Clone, Debug)]
struct WrapLayoutCacheEntry {
    visible_rows: Range<usize>,
    scroll_offset_bits: u32,
    line_height_bits: u32,
    layout: VisibleWrapLayout,
}

#[derive(Clone, Debug)]
pub struct HighlightSnapshot {
    syntax_highlights: Arc<Vec<Highlight>>,
    diagnostics: Arc<Vec<AnchoredDiagnostic>>,
    highlight_indexes_by_line: Arc<Vec<Vec<usize>>>,
    diagnostic_indexes_by_line: Arc<Vec<Vec<usize>>>,
}

#[derive(Clone, Debug)]
pub struct InlaySnapshot {
    inlay_hints: Arc<Vec<AnchoredInlayHint>>,
    inlay_indexes_by_line: Arc<Vec<Vec<usize>>>,
}

#[derive(Clone, Debug)]
pub struct BlockSnapshot {
    block_widgets: Arc<Vec<BlockWidgetChunk>>,
    block_indexes_by_line: Arc<Vec<Vec<usize>>>,
}

#[derive(Clone, Debug)]
struct DisplayChunkSnapshot {
    buffer: BufferSnapshot,
    row_infos: Arc<Vec<RowInfo>>,
    highlight_snapshot: HighlightSnapshot,
    inlay_snapshot: InlaySnapshot,
    block_snapshot: BlockSnapshot,
    block_indexes_by_display_row: Arc<Vec<Vec<usize>>>,
    text_chunk_cache: RefCell<HashMap<usize, DisplayTextChunk>>,
    viewport_cache: RefCell<Option<ViewportCacheEntry>>,
}

#[derive(Clone, Debug)]
struct ViewportCacheEntry {
    visible_rows: Range<usize>,
    viewport: DisplayViewport,
}

#[derive(Clone, Debug, Default)]
struct HighlightMapState {
    total_lines: usize,
    syntax_highlights: Arc<Vec<Highlight>>,
    diagnostics: Arc<Vec<AnchoredDiagnostic>>,
    highlight_indexes_by_line: Arc<Vec<Vec<usize>>>,
    diagnostic_indexes_by_line: Arc<Vec<Vec<usize>>>,
    last_sync_range: Option<Range<usize>>,
}

#[derive(Clone, Debug, Default)]
struct InlayMapState {
    total_lines: usize,
    inlay_hints: Arc<Vec<AnchoredInlayHint>>,
    inlay_indexes_by_line: Arc<Vec<Vec<usize>>>,
    last_sync_range: Option<Range<usize>>,
}

impl DisplayMap {
    pub fn new() -> Self {
        Self::default()
    }

    #[cfg(test)]
    pub(crate) fn from_display_lines(display_lines: Vec<usize>) -> Self {
        let fold_map = FoldMap::from_display_lines(display_lines);
        Self {
            fold_map,
            tab_map: TabMap::default(),
            wrap_map: WrapMap::default(),
            highlight_map: HighlightMap::default(),
            inlay_map: InlayMap::default(),
            block_map: BlockMap::default(),
        }
    }

    pub fn sync(
        &mut self,
        total_lines: usize,
        fold_regions: &[FoldRegion],
        folded_lines: &HashSet<usize>,
    ) {
        self.fold_map.sync(total_lines, fold_regions, folded_lines);
        self.wrap_map.sync(total_lines);
    }

    pub fn sync_range(
        &mut self,
        total_lines: usize,
        fold_regions: &[FoldRegion],
        folded_lines: &HashSet<usize>,
        edited_range: Range<usize>,
    ) {
        self.fold_map.sync_range(
            total_lines,
            fold_regions,
            folded_lines,
            edited_range.clone(),
        );
        self.wrap_map.sync_range(total_lines, edited_range);
    }

    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.fold_map.display_lines()
    }

    pub fn display_line_count(&self) -> usize {
        self.fold_map.display_line_count()
    }

    pub fn set_tab_size(&mut self, tab_size: usize) {
        self.tab_map.set_tab_size(tab_size);
    }

    pub fn last_sync_range(&self) -> Option<Range<usize>> {
        self.fold_map.last_sync_range()
    }

    pub fn last_wrap_sync_range(&self) -> Option<Range<usize>> {
        self.wrap_map.last_sync_range()
    }

    pub fn last_block_sync_range(&self) -> Option<Range<usize>> {
        self.block_map.last_sync_range()
    }

    pub fn update_wrap_rows(
        &mut self,
        buffer_lines: &[usize],
        wrap_column: usize,
        visual_rows: &[usize],
    ) -> bool {
        self.wrap_map
            .update_visual_rows(buffer_lines, wrap_column, visual_rows)
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot(
        &self,
        buffer: BufferSnapshot,
        fold_regions: &[FoldRegion],
        folded_lines: &HashSet<usize>,
        soft_wrap: bool,
        syntax_highlights: Arc<Vec<Highlight>>,
        diagnostics: &[AnchoredDiagnostic],
        inlay_hints: Arc<Vec<AnchoredInlayHint>>,
        code_actions: &[AnchoredCodeAction],
    ) -> DisplaySnapshot {
        let highlight_snapshot =
            self.highlight_map
                .snapshot(&buffer, syntax_highlights.clone(), diagnostics);
        let inlay_snapshot = self.inlay_map.snapshot(&buffer, inlay_hints);
        let block_snapshot =
            self.block_map
                .snapshot(&buffer, buffer.line_count(), diagnostics, code_actions);
        let fold_snapshot = self.fold_map.snapshot(fold_regions, folded_lines);
        let wrap_snapshot = self
            .wrap_map
            .snapshot(soft_wrap, fold_snapshot.display_lines());
        let chunk_snapshot = DisplayChunkSnapshot::new(
            buffer.clone(),
            &fold_snapshot,
            highlight_snapshot.clone(),
            inlay_snapshot.clone(),
            block_snapshot.clone(),
        );

        DisplaySnapshot {
            buffer,
            fold_snapshot,
            tab_snapshot: self.tab_map.snapshot(),
            wrap_snapshot,
            highlight_snapshot,
            inlay_snapshot,
            block_snapshot,
            chunk_snapshot,
        }
    }
}

impl FoldMap {
    #[cfg(test)]
    fn from_display_lines(display_lines: Vec<usize>) -> Self {
        let total_lines = display_lines
            .iter()
            .copied()
            .max()
            .map(|line| line + 1)
            .unwrap_or(0);
        let display_lines = Arc::new(display_lines);
        let row_index = DisplayRowIndex::new(total_lines, display_lines.as_ref());
        Self {
            display_lines,
            row_index,
            last_sync_range: None,
            total_lines,
            fold_regions: Vec::new(),
            folded_lines: HashSet::new(),
        }
    }

    fn sync(
        &mut self,
        total_lines: usize,
        fold_regions: &[FoldRegion],
        folded_lines: &HashSet<usize>,
    ) {
        self.total_lines = total_lines;
        self.fold_regions = fold_regions.to_vec();
        self.folded_lines = folded_lines.clone();

        if folded_lines.is_empty() {
            self.set_display_lines((0..total_lines).collect(), total_lines);
            self.last_sync_range = None;
            return;
        }

        let mut hidden = HashSet::new();
        for region in fold_regions {
            if folded_lines.contains(&region.start_line) {
                for line in (region.start_line + 1)..=region.end_line {
                    if line < total_lines {
                        hidden.insert(line);
                    }
                }
            }
        }

        self.set_display_lines(
            (0..total_lines)
                .filter(|line| !hidden.contains(line))
                .collect(),
            total_lines,
        );
        self.last_sync_range = None;
    }

    fn sync_range(
        &mut self,
        total_lines: usize,
        fold_regions: &[FoldRegion],
        folded_lines: &HashSet<usize>,
        edited_range: Range<usize>,
    ) {
        if self.total_lines == 0 && self.display_lines.is_empty() {
            self.sync(total_lines, fold_regions, folded_lines);
            self.last_sync_range = Some(edited_range);
            return;
        }

        let line_delta = total_lines as isize - self.total_lines as isize;
        let previous_fold_regions = self.fold_regions.clone();
        let previous_folded_lines = self.folded_lines.clone();
        let previous_display_lines = self.display_lines.as_ref().clone();
        let previous_total_lines = self.total_lines;

        let next_range = expand_fold_effect_range(
            clamp_line_range(edited_range.clone(), total_lines),
            total_lines,
            fold_regions,
            folded_lines,
        );
        let previous_range = expand_fold_effect_range(
            edited_range_in_previous_buffer(edited_range.clone(), line_delta, previous_total_lines),
            previous_total_lines,
            &previous_fold_regions,
            &previous_folded_lines,
        );

        let previous_prefix_end = previous_range.start;
        let previous_suffix_start = previous_range.end;

        let mut patched_display_lines = previous_display_lines
            .iter()
            .copied()
            .take_while(|&line| line < previous_prefix_end)
            .collect::<Vec<_>>();
        patched_display_lines.extend(visible_lines_for_range(
            total_lines,
            fold_regions,
            folded_lines,
            next_range.clone(),
        ));
        patched_display_lines.extend(previous_display_lines.iter().filter_map(|&line| {
            if line < previous_suffix_start {
                return None;
            }

            let shifted_line = translate_old_line_to_new(line, line_delta, total_lines);
            (shifted_line >= next_range.end && shifted_line < total_lines).then_some(shifted_line)
        }));

        self.total_lines = total_lines;
        self.fold_regions = fold_regions.to_vec();
        self.folded_lines = folded_lines.clone();
        self.set_display_lines(patched_display_lines, total_lines);
        self.last_sync_range = Some(edited_range);
    }

    fn display_lines(&self) -> Arc<Vec<usize>> {
        self.display_lines.clone()
    }

    fn display_line_count(&self) -> usize {
        self.display_lines.len()
    }

    fn last_sync_range(&self) -> Option<Range<usize>> {
        self.last_sync_range.clone()
    }

    fn snapshot(&self, fold_regions: &[FoldRegion], folded_lines: &HashSet<usize>) -> FoldSnapshot {
        FoldSnapshot {
            display_lines: self.display_lines(),
            row_index: self.row_index.clone(),
            folded_lines: folded_lines.clone(),
            fold_regions: fold_regions.to_vec(),
        }
    }

    fn set_display_lines(&mut self, display_lines: Vec<usize>, total_lines: usize) {
        self.row_index = DisplayRowIndex::new(total_lines, &display_lines);
        self.display_lines = Arc::new(display_lines);
    }
}

impl DisplayRowIndex {
    pub fn new(total_lines: usize, display_lines: &[usize]) -> Self {
        let mut display_slots_by_buffer_line = vec![None; total_lines];
        for (display_slot, &buffer_line) in display_lines.iter().enumerate() {
            if buffer_line < total_lines {
                display_slots_by_buffer_line[buffer_line] = Some(display_slot);
            }
        }

        Self {
            display_slots_by_buffer_line: Arc::new(display_slots_by_buffer_line),
        }
    }

    pub fn display_slot_for_buffer_line(&self, buffer_line: usize) -> Option<usize> {
        self.display_slots_by_buffer_line
            .get(buffer_line)
            .copied()
            .flatten()
    }
}

impl WrapMap {
    fn sync(&mut self, total_lines: usize) {
        if self.total_lines == total_lines && self.visual_rows_by_line.len() == total_lines {
            self.last_sync_range = None;
            return;
        }

        let mut visual_rows_by_line = self.visual_rows_by_line.as_ref().clone();
        visual_rows_by_line.resize(total_lines, 1);
        self.visual_rows_by_line = Arc::new(visual_rows_by_line);
        self.total_lines = total_lines;
        self.last_sync_range = None;
    }

    fn sync_range(&mut self, total_lines: usize, edited_range: Range<usize>) {
        if self.total_lines == 0 && self.visual_rows_by_line.is_empty() {
            self.sync(total_lines);
            self.last_sync_range = Some(edited_range);
            return;
        }

        let previous_visual_rows = self.visual_rows_by_line.as_ref().clone();
        let previous_total_lines = self.total_lines;
        let line_delta = total_lines as isize - previous_total_lines as isize;
        let previous_range =
            edited_range_in_previous_buffer(edited_range.clone(), line_delta, previous_total_lines);
        let mut next_visual_rows = vec![1; total_lines];

        let stable_prefix_end = previous_range
            .start
            .min(previous_total_lines)
            .min(total_lines);
        next_visual_rows[..stable_prefix_end]
            .copy_from_slice(&previous_visual_rows[..stable_prefix_end]);

        for (old_line, &visual_row_count) in previous_visual_rows.iter().enumerate() {
            if old_line < previous_range.end {
                continue;
            }

            let new_line = translate_old_line_to_new(old_line, line_delta, total_lines);
            if new_line >= edited_range.end && new_line < total_lines {
                next_visual_rows[new_line] = visual_row_count;
            }
        }

        self.visual_rows_by_line = Arc::new(next_visual_rows);
        self.total_lines = total_lines;
        self.last_sync_range = Some(edited_range);
    }

    fn update_visual_rows(
        &mut self,
        buffer_lines: &[usize],
        wrap_column: usize,
        visual_rows: &[usize],
    ) -> bool {
        if self.total_lines == 0 {
            return false;
        }

        let mut changed = self.wrap_column != wrap_column;
        let mut changed_lines = Vec::new();
        let mut next_visual_rows = if changed {
            vec![1; self.total_lines]
        } else {
            self.visual_rows_by_line.as_ref().clone()
        };

        for (&buffer_line, &visual_row_count) in buffer_lines.iter().zip(visual_rows.iter()) {
            if let Some(cached_visual_row_count) = next_visual_rows.get_mut(buffer_line) {
                let next_visual_row_count = visual_row_count.max(1);
                if *cached_visual_row_count != next_visual_row_count {
                    *cached_visual_row_count = next_visual_row_count;
                    changed = true;
                    changed_lines.push(buffer_line);
                }
            }
        }

        if !changed {
            return false;
        }

        self.visual_rows_by_line = Arc::new(next_visual_rows);
        self.wrap_column = wrap_column;
        self.last_sync_range = if changed_lines.is_empty() {
            Some(0..self.total_lines)
        } else {
            let start = changed_lines.iter().copied().min().unwrap_or(0);
            let end = changed_lines
                .iter()
                .copied()
                .max()
                .unwrap_or(start)
                .saturating_add(1);
            Some(start..end)
        };
        true
    }

    fn last_sync_range(&self) -> Option<Range<usize>> {
        self.last_sync_range.clone()
    }

    fn snapshot(&self, soft_wrap: bool, display_lines: Arc<Vec<usize>>) -> WrapSnapshot {
        let visual_rows_by_line = if soft_wrap {
            self.visual_rows_by_line.clone()
        } else {
            Arc::new(vec![1; self.total_lines])
        };
        let wrap_column = if soft_wrap { self.wrap_column } else { 0 };
        let mut visual_row_starts_by_display_row = Vec::with_capacity(display_lines.len() + 1);
        let mut display_rows_by_visual_row = Vec::new();
        let mut next_visual_row = 0usize;

        for (display_row, &buffer_line) in display_lines.iter().enumerate() {
            visual_row_starts_by_display_row.push(next_visual_row);
            let row_count = visual_rows_by_line
                .get(buffer_line)
                .copied()
                .unwrap_or(1)
                .max(1);
            display_rows_by_visual_row.extend(std::iter::repeat_n(display_row, row_count));
            next_visual_row = next_visual_row.saturating_add(row_count);
        }
        visual_row_starts_by_display_row.push(next_visual_row);

        WrapSnapshot {
            soft_wrap,
            display_lines,
            visual_rows_by_line,
            visual_row_starts_by_display_row: Arc::new(visual_row_starts_by_display_row),
            display_rows_by_visual_row: Arc::new(display_rows_by_visual_row),
            wrap_column,
            layout_cache: RefCell::new(None),
        }
    }
}

impl TabMap {
    fn set_tab_size(&mut self, tab_size: usize) {
        self.tab_size = tab_size.max(1);
    }

    fn snapshot(&self) -> TabSnapshot {
        TabSnapshot {
            tab_size: self.tab_size.max(1),
        }
    }
}

impl Default for TabMap {
    fn default() -> Self {
        Self { tab_size: 4 }
    }
}

impl HighlightMap {
    fn snapshot(
        &self,
        buffer: &BufferSnapshot,
        syntax_highlights: Arc<Vec<Highlight>>,
        diagnostics: &[AnchoredDiagnostic],
    ) -> HighlightSnapshot {
        let line_count = buffer.line_count();
        let mut state = self.state.borrow_mut();
        let diagnostics = Arc::new(diagnostics.to_vec());

        if state.total_lines != line_count
            || state.syntax_highlights.as_ref() != syntax_highlights.as_ref()
        {
            state.highlight_indexes_by_line = Arc::new(indexes_by_line_for_offsets(
                line_count,
                syntax_highlights
                    .iter()
                    .map(|highlight| highlight.start..highlight.end),
                buffer,
            ));
        }

        if state.total_lines != line_count || state.diagnostics != diagnostics {
            state.diagnostic_indexes_by_line = Arc::new(indexes_by_line_for_offsets(
                line_count,
                diagnostics
                    .iter()
                    .filter_map(|diagnostic| buffer.resolve_anchored_range(diagnostic.range).ok()),
                buffer,
            ));
        }

        state.last_sync_range = if state.total_lines != line_count {
            Some(0..line_count)
        } else {
            None
        };
        state.total_lines = line_count;
        state.syntax_highlights = syntax_highlights.clone();
        state.diagnostics = diagnostics.clone();

        HighlightSnapshot {
            syntax_highlights,
            diagnostics,
            highlight_indexes_by_line: state.highlight_indexes_by_line.clone(),
            diagnostic_indexes_by_line: state.diagnostic_indexes_by_line.clone(),
        }
    }
}

impl InlayMap {
    fn snapshot(
        &self,
        buffer: &BufferSnapshot,
        inlay_hints: Arc<Vec<AnchoredInlayHint>>,
    ) -> InlaySnapshot {
        let mut state = self.state.borrow_mut();
        let line_count = buffer.line_count();

        if state.total_lines != line_count || state.inlay_hints.as_ref() != inlay_hints.as_ref() {
            let mut inlay_indexes_by_line = vec![Vec::new(); line_count];
            for (index, hint) in inlay_hints.iter().enumerate() {
                let Some(line) = buffer
                    .resolve_anchor_position(hint.anchor)
                    .ok()
                    .map(|position| position.line)
                else {
                    continue;
                };
                if let Some(indexes) = inlay_indexes_by_line.get_mut(line) {
                    indexes.push(index);
                }
            }
            state.inlay_indexes_by_line = Arc::new(inlay_indexes_by_line);
        }
        state.last_sync_range = if state.total_lines != line_count {
            Some(0..line_count)
        } else {
            None
        };
        state.total_lines = line_count;
        state.inlay_hints = inlay_hints.clone();

        InlaySnapshot {
            inlay_hints,
            inlay_indexes_by_line: state.inlay_indexes_by_line.clone(),
        }
    }
}

impl BlockMap {
    fn snapshot(
        &self,
        buffer: &BufferSnapshot,
        line_count: usize,
        diagnostics: &[AnchoredDiagnostic],
        code_actions: &[AnchoredCodeAction],
    ) -> BlockSnapshot {
        let mut state = self.state.borrow_mut();
        let block_widgets = block_widgets_from_sources(buffer, diagnostics, code_actions);

        if state.total_lines != line_count || state.block_widgets.as_ref() != &block_widgets {
            state.last_sync_range =
                changed_block_line_range(state.block_widgets.as_ref(), &block_widgets);
            state.block_widgets = Arc::new(block_widgets);
            state.block_indexes_by_line = Arc::new(block_indexes_by_line(
                line_count,
                state.block_widgets.as_ref(),
            ));
            state.total_lines = line_count;
        }

        BlockSnapshot {
            block_widgets: state.block_widgets.clone(),
            block_indexes_by_line: state.block_indexes_by_line.clone(),
        }
    }

    fn last_sync_range(&self) -> Option<Range<usize>> {
        self.state.borrow().last_sync_range.clone()
    }
}

fn block_widgets_from_sources(
    _buffer: &BufferSnapshot,
    _diagnostics: &[AnchoredDiagnostic],
    code_actions: &[AnchoredCodeAction],
) -> Vec<BlockWidgetChunk> {
    let mut blocks = Vec::new();

    for action in code_actions {
        blocks.push(BlockWidgetChunk {
            line: action.line,
            label: action.label.clone(),
        });
    }

    blocks
}

fn block_indexes_by_line(line_count: usize, block_widgets: &[BlockWidgetChunk]) -> Vec<Vec<usize>> {
    let mut block_indexes_by_line = vec![Vec::new(); line_count];
    for (index, block_widget) in block_widgets.iter().enumerate() {
        if let Some(indexes) = block_indexes_by_line.get_mut(block_widget.line) {
            indexes.push(index);
        }
    }
    block_indexes_by_line
}

fn changed_block_line_range(
    previous_widgets: &[BlockWidgetChunk],
    next_widgets: &[BlockWidgetChunk],
) -> Option<Range<usize>> {
    if previous_widgets == next_widgets {
        return None;
    }

    let min_line = previous_widgets
        .iter()
        .chain(next_widgets.iter())
        .map(|widget| widget.line)
        .min()?;
    let max_line = previous_widgets
        .iter()
        .chain(next_widgets.iter())
        .map(|widget| widget.line)
        .max()?;

    Some(min_line..max_line.saturating_add(1))
}

fn indexes_by_line_for_offsets(
    line_count: usize,
    ranges: impl Iterator<Item = Range<usize>>,
    buffer: &BufferSnapshot,
) -> Vec<Vec<usize>> {
    let mut indexes_by_line = vec![Vec::new(); line_count];
    for (index, range) in ranges.enumerate() {
        let Some((start_line, end_line)) = line_span_for_offsets(range, buffer) else {
            continue;
        };

        for line in start_line..=end_line {
            if let Some(line_indexes) = indexes_by_line.get_mut(line) {
                line_indexes.push(index);
            }
        }
    }
    indexes_by_line
}

fn clamp_line_range(range: Range<usize>, total_lines: usize) -> Range<usize> {
    let start = range.start.min(total_lines);
    let end = range.end.min(total_lines);
    start..end.max(start)
}

fn union_ranges(left: Range<usize>, right: Range<usize>) -> Range<usize> {
    left.start.min(right.start)..left.end.max(right.end)
}

fn range_touches(range: &Range<usize>, line_range: &Range<usize>) -> bool {
    if range.start == range.end {
        line_range.start <= range.start && range.start < line_range.end
    } else {
        range.start < line_range.end && line_range.start < range.end
    }
}

fn expand_fold_effect_range(
    range: Range<usize>,
    total_lines: usize,
    fold_regions: &[FoldRegion],
    folded_lines: &HashSet<usize>,
) -> Range<usize> {
    let mut expanded = clamp_line_range(range, total_lines);

    loop {
        let mut changed = false;
        for region in fold_regions {
            if !folded_lines.contains(&region.start_line) {
                continue;
            }

            let effect_range = region.start_line.min(total_lines)
                ..region.end_line.saturating_add(1).min(total_lines);
            if effect_range.start >= effect_range.end || !range_touches(&expanded, &effect_range) {
                continue;
            }

            let merged = union_ranges(expanded.clone(), effect_range);
            if merged != expanded {
                expanded = merged;
                changed = true;
            }
        }

        if !changed {
            break;
        }
    }

    expanded
}

fn translate_old_line_to_new(line: usize, line_delta: isize, total_lines: usize) -> usize {
    if line_delta >= 0 {
        line.saturating_add(line_delta as usize).min(total_lines)
    } else {
        line.saturating_sub((-line_delta) as usize).min(total_lines)
    }
}

fn edited_range_in_previous_buffer(
    edited_range: Range<usize>,
    line_delta: isize,
    previous_total_lines: usize,
) -> Range<usize> {
    let start = edited_range.start.min(previous_total_lines);
    let end = if line_delta >= 0 {
        edited_range.end.saturating_sub(line_delta as usize)
    } else {
        edited_range.end.saturating_add((-line_delta) as usize)
    }
    .min(previous_total_lines);
    start..end.max(start)
}

fn visible_lines_for_range(
    total_lines: usize,
    fold_regions: &[FoldRegion],
    folded_lines: &HashSet<usize>,
    range: Range<usize>,
) -> Vec<usize> {
    let visible_range = clamp_line_range(range, total_lines);
    let hidden_ranges = fold_regions
        .iter()
        .filter(|region| folded_lines.contains(&region.start_line))
        .map(|region| {
            region.start_line.saturating_add(1).min(total_lines)
                ..region.end_line.saturating_add(1).min(total_lines)
        })
        .filter(|hidden_range| hidden_range.start < hidden_range.end)
        .collect::<Vec<_>>();

    (visible_range.start..visible_range.end)
        .filter(|line| {
            !hidden_ranges
                .iter()
                .any(|hidden_range| hidden_range.contains(line))
        })
        .collect()
}

fn line_span_for_offsets(range: Range<usize>, buffer: &BufferSnapshot) -> Option<(usize, usize)> {
    if range.start > buffer.len() {
        return None;
    }

    let start_line = buffer.byte_to_line(range.start.min(buffer.len()))?;
    let inclusive_end = range.end.saturating_sub(1).min(buffer.len());
    let end_line = buffer.byte_to_line(inclusive_end)?;
    Some((start_line.min(end_line), start_line.max(end_line)))
}

impl FoldDisplayState {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn folded_lines(&self) -> &HashSet<usize> {
        &self.folded_lines
    }

    pub fn folded_lines_set(&self) -> HashSet<usize> {
        self.folded_lines.clone()
    }

    pub fn is_line_folded(&self, start_line: usize) -> bool {
        self.folded_lines.contains(&start_line)
    }

    pub fn collapse_line(&mut self, start_line: usize) -> bool {
        self.folded_lines.insert(start_line)
    }

    pub fn expand_line(&mut self, start_line: usize) -> bool {
        self.folded_lines.remove(&start_line)
    }

    pub fn collapse_all(&mut self, fold_regions: &[FoldRegion]) {
        for region in fold_regions {
            self.folded_lines.insert(region.start_line);
        }
    }

    pub fn clear_collapsed_lines(&mut self) {
        self.folded_lines.clear();
    }

    pub fn sync_all(&mut self, total_lines: usize, fold_regions: &[FoldRegion]) {
        self.display_map
            .sync(total_lines, fold_regions, &self.folded_lines);
        self.cached_display_lines = self.display_map.display_lines();
    }

    pub fn sync_range(
        &mut self,
        total_lines: usize,
        fold_regions: &[FoldRegion],
        edited_range: Range<usize>,
    ) {
        self.display_map
            .sync_range(total_lines, fold_regions, &self.folded_lines, edited_range);
        self.cached_display_lines = self.display_map.display_lines();
    }

    pub fn ensure_unfolded_display_lines_cache(&mut self, total_lines: usize) {
        if self.cached_display_lines.len() == total_lines {
            return;
        }

        self.display_map.sync(total_lines, &[], &HashSet::new());
        self.cached_display_lines = self.display_map.display_lines();
    }

    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.cached_display_lines.clone()
    }

    pub fn set_tab_size(&mut self, tab_size: usize) {
        self.display_map.set_tab_size(tab_size);
    }

    pub fn update_wrap_rows(
        &mut self,
        buffer_lines: &[usize],
        wrap_column: usize,
        visual_rows: &[usize],
    ) -> bool {
        self.display_map
            .update_wrap_rows(buffer_lines, wrap_column, visual_rows)
    }

    pub fn visible_buffer_lines(&self) -> &[usize] {
        self.cached_display_lines.as_ref()
    }

    pub fn display_line_count(&self) -> usize {
        self.cached_display_lines.len()
    }

    #[allow(clippy::too_many_arguments)]
    pub fn snapshot(
        &self,
        buffer: BufferSnapshot,
        fold_regions: &[FoldRegion],
        soft_wrap: bool,
        syntax_highlights: Arc<Vec<Highlight>>,
        diagnostics: &[AnchoredDiagnostic],
        inlay_hints: Arc<Vec<AnchoredInlayHint>>,
        code_actions: &[AnchoredCodeAction],
    ) -> DisplaySnapshot {
        self.display_map.snapshot(
            buffer,
            fold_regions,
            &self.folded_lines,
            soft_wrap,
            syntax_highlights,
            diagnostics,
            inlay_hints,
            code_actions,
        )
    }

    pub fn last_sync_range(&self) -> Option<Range<usize>> {
        self.display_map.last_sync_range()
    }

    pub fn last_wrap_sync_range(&self) -> Option<Range<usize>> {
        self.display_map.last_wrap_sync_range()
    }
}

impl FoldSnapshot {
    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.display_lines.clone()
    }

    pub fn folded_lines(&self) -> &HashSet<usize> {
        &self.folded_lines
    }

    pub fn fold_regions(&self) -> &[FoldRegion] {
        &self.fold_regions
    }

    pub fn buffer_line_for_display_slot(&self, display_slot: usize) -> Option<usize> {
        self.display_lines.get(display_slot).copied()
    }

    pub fn display_slot_for_buffer_line(&self, buffer_line: usize) -> Option<usize> {
        self.row_index.display_slot_for_buffer_line(buffer_line)
    }
}

impl WrapSnapshot {
    pub fn soft_wrap(&self) -> bool {
        self.soft_wrap
    }

    pub fn wrap_column(&self) -> usize {
        self.wrap_column
    }

    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.display_lines.clone()
    }

    pub fn visual_rows_for_line(&self, line: usize) -> usize {
        self.visual_rows_by_line.get(line).copied().unwrap_or(1)
    }

    pub fn visual_row_count_for_lines(&self, lines: &[usize]) -> usize {
        lines
            .iter()
            .map(|&line| self.visual_rows_for_line(line).max(1))
            .sum()
    }

    pub fn visual_row_for_display_row(&self, display_row: usize) -> Option<usize> {
        self.visual_row_starts_by_display_row
            .get(display_row)
            .copied()
    }

    pub fn display_row_for_visual_row(&self, visual_row: usize) -> Option<usize> {
        self.display_rows_by_visual_row.get(visual_row).copied()
    }

    pub fn wrap_subrow_for_column(&self, column: usize) -> usize {
        if self.wrap_column == 0 {
            0
        } else {
            column / self.wrap_column
        }
    }

    pub fn column_for_wrap_subrow(&self, wrap_subrow: usize, column_within_subrow: usize) -> usize {
        if self.wrap_column == 0 {
            column_within_subrow
        } else {
            wrap_subrow
                .saturating_mul(self.wrap_column)
                .saturating_add(column_within_subrow)
        }
    }

    pub fn layout_for_rows(
        &self,
        rows: Range<usize>,
        scroll_offset: f32,
        line_height: Pixels,
    ) -> VisibleWrapLayout {
        let scroll_offset_bits = scroll_offset.to_bits();
        let line_height_bits = f32::from(line_height).to_bits();
        if let Some(cached_layout) = self
            .layout_cache
            .borrow()
            .as_ref()
            .filter(|entry| {
                entry.visible_rows == rows
                    && entry.scroll_offset_bits == scroll_offset_bits
                    && entry.line_height_bits == line_height_bits
            })
            .map(|entry| entry.layout.clone())
        {
            return cached_layout;
        }

        let visual_rows = rows
            .clone()
            .filter_map(|display_row| self.display_lines.get(display_row).copied())
            .map(|buffer_line| self.visual_rows_for_line(buffer_line))
            .collect::<Vec<_>>();

        let layout = VisibleWrapLayout::new(
            rows,
            visual_rows,
            self.wrap_column,
            scroll_offset,
            line_height,
        );
        *self.layout_cache.borrow_mut() = Some(WrapLayoutCacheEntry {
            visible_rows: layout.visible_rows.clone(),
            scroll_offset_bits,
            line_height_bits,
            layout: layout.clone(),
        });
        layout
    }
}

impl VisibleWrapLayout {
    pub fn new(
        visible_rows: Range<usize>,
        visual_rows: Vec<usize>,
        wrap_column: usize,
        scroll_offset: f32,
        line_height: Pixels,
    ) -> Self {
        let sub_line_offset = (scroll_offset - visible_rows.start as f32) * line_height;
        let mut line_y_offsets = Vec::with_capacity(visual_rows.len());
        let mut y = -sub_line_offset;

        for &row_count in &visual_rows {
            line_y_offsets.push(y);
            y += line_height * row_count.max(1) as f32;
        }

        Self {
            visible_rows,
            line_y_offsets: Arc::new(line_y_offsets),
            visual_rows: Arc::new(visual_rows),
            wrap_column,
        }
    }

    pub fn visible_rows(&self) -> Range<usize> {
        self.visible_rows.clone()
    }

    pub fn line_y_offsets(&self) -> Arc<Vec<Pixels>> {
        self.line_y_offsets.clone()
    }

    pub fn visual_rows(&self) -> Arc<Vec<usize>> {
        self.visual_rows.clone()
    }

    pub fn wrap_column(&self) -> usize {
        self.wrap_column
    }

    pub fn line_y_offset_for_slot(&self, display_slot: usize) -> Option<Pixels> {
        let visible_index = display_slot.checked_sub(self.visible_rows.start)?;
        self.line_y_offsets.get(visible_index).copied()
    }

    pub fn visual_rows_for_slot(&self, display_slot: usize) -> Option<usize> {
        let visible_index = display_slot.checked_sub(self.visible_rows.start)?;
        self.visual_rows.get(visible_index).copied()
    }

    pub fn display_slot_and_subrow_for_y(
        &self,
        y: Pixels,
        line_height: Pixels,
    ) -> Option<(usize, usize)> {
        let mut visible_index = None;

        for (index, start_y) in self.line_y_offsets.iter().enumerate() {
            let visual_rows = self.visual_rows.get(index).copied().unwrap_or(1).max(1);
            let end_y = *start_y + line_height * visual_rows as f32;
            if y >= *start_y && y < end_y {
                visible_index = Some(index);
                break;
            }
            if y >= end_y {
                visible_index = Some(index);
            }
        }

        let visible_index = visible_index?;
        let display_slot = self.visible_rows.start + visible_index;
        let row_start_y = self.line_y_offsets.get(visible_index).copied()?;
        let wrap_subrow = ((y - row_start_y) / line_height).max(0.0) as usize;
        Some((display_slot, wrap_subrow))
    }
}

impl TabSnapshot {
    pub fn tab_size(&self) -> usize {
        self.tab_size.max(1)
    }

    pub fn display_column_for_text_column(&self, text: &str, text_column: usize) -> usize {
        let mut display_column = 0usize;
        for character in text.chars().take(text_column) {
            display_column = match character {
                '\t' => advance_to_next_tab_stop(display_column, self.tab_size()),
                _ => display_column.saturating_add(1),
            };
        }
        display_column
    }

    pub fn text_column_for_display_column(&self, text: &str, display_column: usize) -> usize {
        let mut text_column = 0usize;
        let mut resolved_display_column = 0usize;

        for character in text.chars() {
            let next_display_column = match character {
                '\t' => advance_to_next_tab_stop(resolved_display_column, self.tab_size()),
                _ => resolved_display_column.saturating_add(1),
            };
            if display_column < next_display_column {
                break;
            }

            resolved_display_column = next_display_column;
            text_column = text_column.saturating_add(1);
        }

        text_column
    }
}

impl HighlightSnapshot {
    pub fn syntax_highlights(&self) -> Arc<Vec<Highlight>> {
        self.syntax_highlights.clone()
    }

    pub fn diagnostics(&self) -> &[AnchoredDiagnostic] {
        &self.diagnostics
    }

    pub fn highlight_indexes_for_line(&self, line: usize) -> &[usize] {
        self.highlight_indexes_by_line
            .get(line)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }

    pub fn diagnostic_indexes_for_line(&self, line: usize) -> &[usize] {
        self.diagnostic_indexes_by_line
            .get(line)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl InlaySnapshot {
    pub fn inlay_hints(&self) -> Arc<Vec<AnchoredInlayHint>> {
        self.inlay_hints.clone()
    }

    pub fn inlay_indexes_for_line(&self, line: usize) -> &[usize] {
        self.inlay_indexes_by_line
            .get(line)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl BlockSnapshot {
    pub fn block_widgets(&self) -> &[BlockWidgetChunk] {
        self.block_widgets.as_ref()
    }

    pub fn block_indexes_for_line(&self, line: usize) -> &[usize] {
        self.block_indexes_by_line
            .get(line)
            .map(Vec::as_slice)
            .unwrap_or(&[])
    }
}

impl DisplayChunkSnapshot {
    fn new(
        buffer: BufferSnapshot,
        fold_snapshot: &FoldSnapshot,
        highlight_snapshot: HighlightSnapshot,
        inlay_snapshot: InlaySnapshot,
        block_snapshot: BlockSnapshot,
    ) -> Self {
        let row_infos: Arc<Vec<RowInfo>> = Arc::new(
            fold_snapshot
                .display_lines()
                .iter()
                .enumerate()
                .map(|(display_row, &buffer_line)| RowInfo {
                    row_id: DisplayRowId {
                        buffer_line,
                        wrap_subrow: 0,
                    },
                    display_row,
                    buffer_line,
                })
                .collect(),
        );

        let mut block_indexes_by_display_row: Vec<Vec<usize>> = vec![Vec::new(); row_infos.len()];
        for (display_row, row_info) in row_infos.iter().enumerate() {
            if let Some(indexes) = block_indexes_by_display_row.get_mut(display_row) {
                indexes.extend(block_snapshot.block_indexes_for_line(row_info.buffer_line));
            }
        }

        Self {
            buffer,
            row_infos,
            highlight_snapshot,
            inlay_snapshot,
            block_snapshot,
            block_indexes_by_display_row: Arc::new(block_indexes_by_display_row),
            text_chunk_cache: RefCell::new(HashMap::new()),
            viewport_cache: RefCell::new(None),
        }
    }

    fn row_infos(&self, rows: Range<usize>) -> Vec<RowInfo> {
        let clamped = rows.start.min(self.row_infos.len())..rows.end.min(self.row_infos.len());
        self.row_infos[clamped].to_vec()
    }

    fn text_chunks(&self, rows: Range<usize>) -> Vec<DisplayTextChunk> {
        self.row_infos(rows)
            .into_iter()
            .map(|row_info| self.text_chunk_for_row(row_info.display_row, row_info))
            .collect()
    }

    fn block_widgets_for_rows(&self, rows: Range<usize>) -> Vec<(usize, &BlockWidgetChunk)> {
        let clamped = rows.start.min(self.row_infos.len())..rows.end.min(self.row_infos.len());
        let mut block_widgets = Vec::new();

        for display_row in clamped {
            for &index in self
                .block_indexes_by_display_row
                .get(display_row)
                .map(Vec::as_slice)
                .unwrap_or(&[])
            {
                if let Some(block_widget) = self.block_snapshot.block_widgets.as_ref().get(index) {
                    block_widgets.push((display_row, block_widget));
                }
            }
        }

        block_widgets
    }

    fn viewport(&self, rows: Range<usize>) -> DisplayViewport {
        if let Some(viewport) = self
            .viewport_cache
            .borrow()
            .as_ref()
            .filter(|entry| entry.visible_rows == rows)
            .map(|entry| entry.viewport.clone())
        {
            return viewport;
        }

        let row_infos = Arc::new(self.row_infos(rows.clone()));
        let text_chunks = Arc::new(self.text_chunks(rows.clone()));
        let block_widgets = Arc::new(
            self.block_widgets_for_rows(rows.clone())
                .into_iter()
                .map(|(display_row, block)| (display_row, block.clone()))
                .collect(),
        );
        let viewport = DisplayViewport {
            visible_rows: rows.clone(),
            row_infos,
            text_chunks,
            block_widgets,
        };

        *self.viewport_cache.borrow_mut() = Some(ViewportCacheEntry {
            visible_rows: rows,
            viewport: viewport.clone(),
        });
        viewport
    }

    fn text_chunk_for_row(&self, display_row: usize, row_info: RowInfo) -> DisplayTextChunk {
        if let Some(chunk) = self.text_chunk_cache.borrow().get(&display_row).cloned() {
            return chunk;
        }

        let line_text = self
            .buffer
            .line(row_info.buffer_line)
            .unwrap_or_default()
            .trim_end_matches('\n')
            .trim_end_matches('\r')
            .to_string();
        let start_offset = self.buffer.line_to_byte(row_info.buffer_line).unwrap_or(0);
        let end_offset = start_offset + line_text.len();

        let highlights = self
            .highlight_snapshot
            .highlight_indexes_for_line(row_info.buffer_line)
            .iter()
            .filter_map(|&highlight_index| {
                let highlight = self
                    .highlight_snapshot
                    .syntax_highlights
                    .get(highlight_index)?;
                if highlight.end <= start_offset || highlight.start >= end_offset {
                    return None;
                }

                Some(ChunkHighlight {
                    start: highlight.start.saturating_sub(start_offset),
                    end: highlight.end.min(end_offset).saturating_sub(start_offset),
                    kind: highlight.kind,
                })
            })
            .collect();

        let diagnostics = self
            .highlight_snapshot
            .diagnostic_indexes_for_line(row_info.buffer_line)
            .iter()
            .filter_map(|&diagnostic_index| {
                let diagnostic = self.highlight_snapshot.diagnostics.get(diagnostic_index)?;
                let range = self.buffer.resolve_anchored_range(diagnostic.range).ok()?;
                if range.end <= start_offset || range.start >= end_offset {
                    return None;
                }

                Some(
                    range.start.saturating_sub(start_offset)
                        ..range.end.min(end_offset).saturating_sub(start_offset),
                )
            })
            .collect();

        let inlay_hints = self
            .inlay_snapshot
            .inlay_indexes_for_line(row_info.buffer_line)
            .iter()
            .filter_map(|&hint_index| self.inlay_snapshot.inlay_hints.get(hint_index).cloned())
            .collect();

        let chunk = DisplayTextChunk {
            row_id: row_info.row_id,
            display_row: row_info.display_row,
            buffer_line: row_info.buffer_line,
            start_offset,
            text: line_text,
            highlights,
            diagnostics,
            inlay_hints,
        };
        self.text_chunk_cache
            .borrow_mut()
            .insert(display_row, chunk.clone());
        chunk
    }
}

impl DisplayViewport {
    pub fn visible_rows(&self) -> Range<usize> {
        self.visible_rows.clone()
    }

    pub fn row_infos(&self) -> Arc<Vec<RowInfo>> {
        self.row_infos.clone()
    }

    pub fn text_chunks(&self) -> Arc<Vec<DisplayTextChunk>> {
        self.text_chunks.clone()
    }

    pub fn block_widgets(&self) -> Arc<Vec<(usize, BlockWidgetChunk)>> {
        self.block_widgets.clone()
    }
}

impl DisplaySnapshot {
    pub fn buffer(&self) -> &BufferSnapshot {
        &self.buffer
    }

    pub fn fold_snapshot(&self) -> &FoldSnapshot {
        &self.fold_snapshot
    }

    pub fn wrap_snapshot(&self) -> &WrapSnapshot {
        &self.wrap_snapshot
    }

    pub fn tab_snapshot(&self) -> &TabSnapshot {
        &self.tab_snapshot
    }

    pub fn highlight_snapshot(&self) -> &HighlightSnapshot {
        &self.highlight_snapshot
    }

    pub fn inlay_snapshot(&self) -> &InlaySnapshot {
        &self.inlay_snapshot
    }

    pub fn block_snapshot(&self) -> &BlockSnapshot {
        &self.block_snapshot
    }

    pub fn display_lines(&self) -> Arc<Vec<usize>> {
        self.fold_snapshot.display_lines.clone()
    }

    pub fn visible_buffer_lines(&self) -> &[usize] {
        self.fold_snapshot.display_lines.as_ref()
    }

    pub fn display_line_count(&self) -> usize {
        self.fold_snapshot.display_lines.len()
    }

    pub fn visual_display_row_count(&self) -> usize {
        self.wrap_snapshot
            .visual_row_count_for_lines(self.visible_buffer_lines())
    }

    pub fn folded_lines(&self) -> &HashSet<usize> {
        &self.fold_snapshot.folded_lines
    }

    pub fn fold_regions(&self) -> &[FoldRegion] {
        &self.fold_snapshot.fold_regions
    }

    pub fn soft_wrap(&self) -> bool {
        self.wrap_snapshot.soft_wrap
    }

    pub fn diagnostics(&self) -> &[AnchoredDiagnostic] {
        &self.highlight_snapshot.diagnostics
    }

    pub fn inlay_hints(&self) -> Arc<Vec<AnchoredInlayHint>> {
        self.inlay_snapshot.inlay_hints.clone()
    }

    pub fn block_widgets(&self) -> &[BlockWidgetChunk] {
        self.block_snapshot.block_widgets()
    }

    pub fn block_widgets_for_line(&self, line: usize) -> Vec<&BlockWidgetChunk> {
        self.block_snapshot
            .block_indexes_for_line(line)
            .iter()
            .filter_map(|&index| self.block_snapshot.block_widgets.as_ref().get(index))
            .collect()
    }

    pub fn block_widgets_for_rows(&self, rows: Range<usize>) -> Vec<(usize, &BlockWidgetChunk)> {
        self.chunk_snapshot.block_widgets_for_rows(rows)
    }

    pub fn viewport(&self, rows: Range<usize>) -> DisplayViewport {
        self.chunk_snapshot.viewport(rows)
    }

    pub fn text_chunks(&self, rows: Range<usize>) -> Vec<DisplayTextChunk> {
        self.chunk_snapshot.text_chunks(rows)
    }

    pub fn reverse_text_chunks(&self, rows: Range<usize>) -> Vec<DisplayTextChunk> {
        let mut chunks = self.text_chunks(rows);
        chunks.reverse();
        chunks
    }

    pub fn sticky_header_excerpt(&self, top_row: usize) -> Option<StickyHeaderChunk> {
        let top_line = self.buffer_line_for_display_slot(top_row)?;
        let region = self
            .fold_snapshot
            .fold_regions
            .iter()
            .filter(|region| region.start_line < top_line && region.end_line >= top_line)
            .max_by_key(|region| region.start_line)?;
        let line_text = self
            .buffer
            .line(region.start_line)
            .unwrap_or_default()
            .trim_end_matches('\n')
            .trim_end_matches('\r')
            .trim()
            .to_string();

        if line_text.is_empty() {
            return None;
        }

        Some(StickyHeaderChunk {
            buffer_line: region.start_line,
            display_row: self
                .display_slot_for_buffer_line(region.start_line)
                .unwrap_or(region.start_line),
            text: line_text,
            kind_label: region.kind.label().to_string(),
        })
    }

    pub fn buffer_line_for_display_slot(&self, display_slot: usize) -> Option<usize> {
        self.fold_snapshot.display_lines.get(display_slot).copied()
    }

    pub fn display_slot_for_buffer_line(&self, buffer_line: usize) -> Option<usize> {
        self.fold_snapshot
            .row_index
            .display_slot_for_buffer_line(buffer_line)
    }

    pub fn display_slot_for_buffer_line_in_rows(
        &self,
        buffer_line: usize,
        rows: &Range<usize>,
    ) -> Option<usize> {
        self.display_slot_for_buffer_line(buffer_line)
            .filter(|display_slot| rows.contains(display_slot))
    }

    pub fn visible_byte_range(
        &self,
        scroll_offset: f32,
        viewport_lines: usize,
    ) -> Option<Range<usize>> {
        let display_line_count = self.display_line_count();
        if display_line_count == 0 {
            return None;
        }

        let start_slot = scroll_offset.floor().max(0.0) as usize;
        let end_slot = (start_slot + viewport_lines.max(1)).min(display_line_count);
        if start_slot >= end_slot {
            return None;
        }

        let start_line = self.buffer_line_for_display_slot(start_slot)?;
        let end_line = self.buffer_line_for_display_slot(end_slot.saturating_sub(1))?;
        let start_offset = self.buffer.line_to_byte(start_line)?;
        let end_offset = if end_line + 1 < self.buffer.line_count() {
            self.buffer.line_to_byte(end_line + 1)?
        } else {
            self.buffer.len()
        };

        Some(start_offset..end_offset)
    }

    pub fn point_to_display_point(&self, point: Position) -> Option<DisplayPoint> {
        self.point_to_row_id(point).and_then(|row_id| {
            self.display_slot_for_buffer_line(point.line)
                .map(|row| DisplayPoint {
                    row,
                    column: self
                        .display_column_for_position(point)
                        .unwrap_or(point.column),
                })
                .filter(|_| row_id.buffer_line == point.line)
        })
    }

    pub fn point_to_row_id(&self, point: Position) -> Option<DisplayRowId> {
        self.display_slot_for_buffer_line(point.line)?;
        let display_column = self.display_column_for_position(point)?;
        Some(DisplayRowId {
            buffer_line: point.line,
            wrap_subrow: self.wrap_snapshot.wrap_subrow_for_column(display_column),
        })
    }

    pub fn row_id_for_display_slot(&self, display_slot: usize) -> Option<DisplayRowId> {
        let buffer_line = self.buffer_line_for_display_slot(display_slot)?;
        Some(DisplayRowId {
            buffer_line,
            wrap_subrow: 0,
        })
    }

    pub fn display_slot_for_row_id(&self, row_id: DisplayRowId) -> Option<usize> {
        let display_row = self.display_slot_for_buffer_line(row_id.buffer_line)?;
        let visual_row_count = self
            .wrap_snapshot
            .visual_rows_for_line(row_id.buffer_line)
            .max(1);
        (row_id.wrap_subrow < visual_row_count).then_some(display_row)
    }

    pub fn position_for_row_id(
        &self,
        row_id: DisplayRowId,
        column_within_subrow: usize,
    ) -> Position {
        let display_column = self
            .wrap_snapshot
            .column_for_wrap_subrow(row_id.wrap_subrow, column_within_subrow);
        self.position_for_display_column(row_id.buffer_line, display_column)
    }

    pub fn display_point_to_point(&self, display_point: DisplayPoint) -> Position {
        let buffer_line = self
            .buffer_line_for_display_slot(display_point.row)
            .or_else(|| self.visible_buffer_lines().last().copied())
            .unwrap_or(0);
        self.position_for_display_column(buffer_line, display_point.column)
    }

    pub fn display_column_for_position(&self, position: Position) -> Option<usize> {
        let line_text = self.buffer.line(position.line)?;
        Some(
            self.tab_snapshot
                .display_column_for_text_column(&line_text, position.column),
        )
    }

    pub fn position_for_display_column(
        &self,
        buffer_line: usize,
        display_column: usize,
    ) -> Position {
        let text_column = self
            .buffer
            .line(buffer_line)
            .map(|line_text| {
                self.tab_snapshot
                    .text_column_for_display_column(&line_text, display_column)
            })
            .unwrap_or(display_column);
        self.buffer
            .clamp_position(Position::new(buffer_line, text_column))
    }

    pub fn row_infos(&self, rows: Range<usize>) -> Vec<RowInfo> {
        self.chunk_snapshot.row_infos(rows)
    }

    pub fn visible_rows(&self, start_row: usize, end_row: usize) -> Vec<RowInfo> {
        self.row_infos(start_row..end_row.min(self.display_line_count()))
    }

    pub fn visual_row_infos(&self, rows: Range<usize>) -> Vec<VisualRowInfo> {
        rows.filter_map(|visual_row| {
            let display_row = self.wrap_snapshot.display_row_for_visual_row(visual_row)?;
            let buffer_line = self.buffer_line_for_display_slot(display_row)?;
            let visual_row_start = self.wrap_snapshot.visual_row_for_display_row(display_row)?;
            Some(VisualRowInfo {
                row_id: DisplayRowId {
                    buffer_line,
                    wrap_subrow: visual_row.saturating_sub(visual_row_start),
                },
                visual_row,
                display_row,
                buffer_line,
            })
        })
        .collect()
    }

    pub fn visual_row_id_for_position(&self, point: Position) -> Option<VisualRowInfo> {
        let row_id = self.point_to_row_id(point)?;
        let display_row = self.display_slot_for_row_id(row_id)?;
        let visual_row = self.visual_row_for_row_id(row_id)?;
        Some(VisualRowInfo {
            row_id,
            visual_row,
            display_row,
            buffer_line: row_id.buffer_line,
        })
    }

    pub fn visual_row_for_row_id(&self, row_id: DisplayRowId) -> Option<usize> {
        let display_row = self.display_slot_for_row_id(row_id)?;
        self.wrap_snapshot
            .visual_row_for_display_row(display_row)
            .map(|visual_row| visual_row + row_id.wrap_subrow)
    }
}

fn advance_to_next_tab_stop(display_column: usize, tab_size: usize) -> usize {
    let tab_size = tab_size.max(1);
    let remainder = display_column % tab_size;
    if remainder == 0 {
        display_column.saturating_add(tab_size)
    } else {
        display_column.saturating_add(tab_size - remainder)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Highlight, HighlightKind, InlayHintSide,
        buffer::{Bias, TextBuffer},
    };

    fn test_wrap_snapshot(visual_rows_by_line: Vec<usize>, wrap_column: usize) -> WrapSnapshot {
        let total_lines = visual_rows_by_line.len();
        let mut visual_row_starts_by_display_row = Vec::with_capacity(total_lines + 1);
        let mut display_rows_by_visual_row = Vec::new();
        let mut next_visual_row = 0usize;

        for display_row in 0..total_lines {
            visual_row_starts_by_display_row.push(next_visual_row);
            let row_count = visual_rows_by_line
                .get(display_row)
                .copied()
                .unwrap_or(1)
                .max(1);
            display_rows_by_visual_row.extend(std::iter::repeat_n(display_row, row_count));
            next_visual_row = next_visual_row.saturating_add(row_count);
        }
        visual_row_starts_by_display_row.push(next_visual_row);

        WrapSnapshot {
            soft_wrap: true,
            display_lines: Arc::new((0..total_lines).collect()),
            visual_rows_by_line: Arc::new(visual_rows_by_line),
            visual_row_starts_by_display_row: Arc::new(visual_row_starts_by_display_row),
            display_rows_by_visual_row: Arc::new(display_rows_by_visual_row),
            wrap_column,
            layout_cache: RefCell::new(None),
        }
    }

    #[test]
    fn test_text_chunks_clip_highlights_to_visible_line() {
        let buffer = TextBuffer::new("alpha\nbeta");
        let display_map = DisplayMap::from_display_lines(vec![0, 1]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(vec![Highlight {
                start: 1,
                end: 4,
                kind: HighlightKind::Keyword,
            }]),
            &[],
            Arc::new(vec![]),
            &[],
        );

        let chunks = snapshot.text_chunks(0..1);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].text, "alpha");
        assert_eq!(
            chunks[0].highlights,
            vec![ChunkHighlight {
                start: 1,
                end: 4,
                kind: HighlightKind::Keyword
            }]
        );
    }

    #[test]
    fn test_reverse_text_chunks_follow_display_order() {
        let buffer = TextBuffer::new("one\ntwo\nthree");
        let display_map = DisplayMap::from_display_lines(vec![0, 2]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(vec![AnchoredInlayHint {
                anchor: buffer.anchor_at(8, Bias::Right).expect("inlay anchor"),
                label: "hint".to_string(),
                side: InlayHintSide::After,
                kind: None,
                padding_left: false,
                padding_right: false,
            }]),
            &[],
        );

        let chunks = snapshot.reverse_text_chunks(0..2);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].buffer_line, 2);
        assert_eq!(chunks[1].buffer_line, 0);
        assert_eq!(chunks[0].inlay_hints.len(), 1);
    }

    #[test]
    fn test_sticky_header_excerpt_uses_enclosing_fold_region() {
        let buffer = TextBuffer::new("BEGIN\n  SELECT 1\nEND\nSELECT 2");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[FoldRegion::new(0, 2, crate::FoldKind::Block)],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let header = snapshot.sticky_header_excerpt(1).expect("sticky header");
        assert_eq!(header.buffer_line, 0);
        assert_eq!(header.text, "BEGIN");
        assert_eq!(header.kind_label, "block");
    }

    #[test]
    fn test_block_widgets_are_exposed_by_display_snapshot() {
        let buffer = TextBuffer::new("SELECT 1");
        let display_map = DisplayMap::from_display_lines(vec![0]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[AnchoredCodeAction {
                line: 0,
                label: "Quick fix".to_string(),
            }],
        );

        assert_eq!(snapshot.block_widgets().len(), 1);
        assert_eq!(snapshot.block_widgets()[0].label, "Quick fix");
    }

    #[test]
    fn diagnostics_do_not_create_block_widgets() {
        let buffer = TextBuffer::new("SELECT 1");
        let display_map = DisplayMap::from_display_lines(vec![0]);
        let diagnostic_range = buffer
            .snapshot()
            .anchored_range(0..6, crate::Bias::Left, crate::Bias::Right)
            .expect("anchored diagnostic");
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[AnchoredDiagnostic {
                range: diagnostic_range,
                kind: HighlightKind::Error,
            }],
            Arc::new(Vec::new()),
            &[],
        );

        assert!(snapshot.block_widgets().is_empty());
    }

    #[test]
    fn fold_display_state_tracks_collapsed_lines_and_visible_rows() {
        let mut state = FoldDisplayState::new();
        let folds = vec![FoldRegion::new(1, 3, crate::FoldKind::Block)];

        assert!(state.collapse_line(1));
        assert!(state.is_line_folded(1));

        state.sync_all(5, &folds);
        assert_eq!(state.visible_buffer_lines(), &[0, 1, 4]);

        state.expand_line(1);
        state.sync_all(5, &folds);
        assert_eq!(state.visible_buffer_lines(), &[0, 1, 2, 3, 4]);
    }

    #[test]
    fn fold_display_state_sync_range_records_last_sync_range() {
        let mut state = FoldDisplayState::new();
        state.sync_range(6, &[], 2..4);

        assert_eq!(state.last_sync_range(), Some(2..4));
    }

    #[test]
    fn fold_map_sync_range_patches_local_fold_region_changes() {
        let mut display_map = DisplayMap::new();
        let folded_lines = HashSet::from([1]);

        display_map.sync(
            6,
            &[FoldRegion::new(1, 3, crate::FoldKind::Block)],
            &folded_lines,
        );
        display_map.sync_range(
            7,
            &[FoldRegion::new(1, 4, crate::FoldKind::Block)],
            &folded_lines,
            2..5,
        );

        assert_eq!(display_map.display_lines().as_ref(), &vec![0, 1, 5, 6]);
        assert_eq!(display_map.last_sync_range(), Some(2..5));
    }

    #[test]
    fn fold_map_sync_range_matches_full_sync_for_edited_suffix() {
        let folded_lines = HashSet::from([1, 6]);
        let initial_regions = vec![
            FoldRegion::new(1, 3, crate::FoldKind::Block),
            FoldRegion::new(6, 8, crate::FoldKind::Block),
        ];
        let next_regions = vec![
            FoldRegion::new(1, 3, crate::FoldKind::Block),
            FoldRegion::new(7, 9, crate::FoldKind::Block),
        ];

        let mut incremental = DisplayMap::new();
        incremental.sync(9, &initial_regions, &folded_lines);
        incremental.sync_range(10, &next_regions, &folded_lines, 6..10);

        let mut full = DisplayMap::new();
        full.sync(10, &next_regions, &folded_lines);

        assert_eq!(
            incremental.display_lines().as_ref(),
            full.display_lines().as_ref()
        );
    }

    #[test]
    fn display_snapshot_uses_indexed_buffer_line_lookup() {
        let buffer = TextBuffer::new("one\ntwo\nthree\nfour");
        let display_map = DisplayMap::from_display_lines(vec![0, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.display_slot_for_buffer_line(2), Some(1));
        assert_eq!(snapshot.display_slot_for_buffer_line(1), None);
    }

    #[test]
    fn display_row_index_tracks_visible_slots() {
        let index = DisplayRowIndex::new(5, &[0, 2, 4]);

        assert_eq!(index.display_slot_for_buffer_line(0), Some(0));
        assert_eq!(index.display_slot_for_buffer_line(2), Some(1));
        assert_eq!(index.display_slot_for_buffer_line(1), None);
    }

    #[test]
    fn highlight_and_inlay_indexes_are_bounded_per_line() {
        let buffer = TextBuffer::new("alpha\nbeta\ngamma");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(vec![Highlight {
                start: 0,
                end: 5,
                kind: HighlightKind::Keyword,
            }]),
            &[AnchoredDiagnostic {
                range: buffer
                    .anchored_range(6..10, Bias::Left, Bias::Right)
                    .expect("diagnostic range"),
                kind: HighlightKind::Error,
            }],
            Arc::new(vec![AnchoredInlayHint {
                anchor: buffer.anchor_at(11, Bias::Right).expect("inlay anchor"),
                label: "hint".to_string(),
                side: InlayHintSide::After,
                kind: None,
                padding_left: false,
                padding_right: false,
            }]),
            &[AnchoredCodeAction {
                line: 2,
                label: "Widget".to_string(),
            }],
        );

        assert_eq!(
            snapshot.highlight_snapshot().highlight_indexes_for_line(0),
            &[0]
        );
        assert!(
            snapshot
                .highlight_snapshot()
                .highlight_indexes_for_line(1)
                .is_empty()
        );
        assert_eq!(
            snapshot.highlight_snapshot().diagnostic_indexes_for_line(1),
            &[0]
        );
        assert_eq!(snapshot.inlay_snapshot().inlay_indexes_for_line(2), &[0]);
        assert_eq!(snapshot.block_snapshot().block_indexes_for_line(2), &[0]);
    }

    #[test]
    fn display_snapshot_limits_line_lookup_to_visible_rows() {
        let buffer = TextBuffer::new("zero\none\ntwo\nthree");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(
            snapshot.display_slot_for_buffer_line_in_rows(1, &(1..3)),
            Some(1)
        );
        assert_eq!(
            snapshot.display_slot_for_buffer_line_in_rows(3, &(1..3)),
            None
        );
    }

    #[test]
    fn display_snapshot_collects_block_widgets_by_visible_rows() {
        let buffer = TextBuffer::new("zero\none\ntwo\nthree");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[
                AnchoredCodeAction {
                    line: 1,
                    label: "one".to_string(),
                },
                AnchoredCodeAction {
                    line: 3,
                    label: "three".to_string(),
                },
            ],
        );

        let widgets = snapshot.block_widgets_for_rows(1..3);
        assert_eq!(widgets.len(), 1);
        assert_eq!(widgets[0].0, 1);
        assert_eq!(widgets[0].1.label, "one");
    }

    #[test]
    fn row_infos_and_text_chunks_carry_stable_row_ids() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let display_map = DisplayMap::from_display_lines(vec![0, 2]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let rows = snapshot.row_infos(0..2);
        assert_eq!(
            rows[0].row_id,
            DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0,
            }
        );
        assert_eq!(
            rows[1].row_id,
            DisplayRowId {
                buffer_line: 2,
                wrap_subrow: 0,
            }
        );

        let chunks = snapshot.text_chunks(0..2);
        assert_eq!(chunks[0].row_id, rows[0].row_id);
        assert_eq!(chunks[1].row_id, rows[1].row_id);
    }

    #[test]
    fn display_snapshot_visible_byte_range_tracks_viewport_rows() {
        let buffer = TextBuffer::new("alpha\nbeta\ngamma\ndelta");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.visible_byte_range(1.0, 2), Some(6..17));
    }

    #[test]
    fn point_to_row_id_uses_wrap_subrow_from_wrap_column() {
        let mut display_map = DisplayMap::from_display_lines(vec![0]);
        display_map.sync(1, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0], 4, &[3]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("abcdefghij").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(
            snapshot.point_to_row_id(Position::new(0, 9)),
            Some(DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 2,
            })
        );
    }

    #[test]
    fn position_for_row_id_reconstructs_wrapped_column() {
        let mut display_map = DisplayMap::from_display_lines(vec![0]);
        display_map.sync(1, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0], 4, &[3]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("abcdefghij").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let position = snapshot.position_for_row_id(
            DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 2,
            },
            1,
        );
        assert_eq!(position, Position::new(0, 9));
    }

    #[test]
    fn tab_snapshot_expands_tabs_for_display_coordinate_conversion() {
        let buffer = TextBuffer::new("\talpha");
        let mut display_map = DisplayMap::from_display_lines(vec![0]);
        display_map.set_tab_size(4);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.tab_snapshot().tab_size(), 4);
        assert_eq!(
            snapshot.point_to_display_point(Position::new(0, 1)),
            Some(DisplayPoint { row: 0, column: 4 })
        );
        assert_eq!(
            snapshot.display_point_to_point(DisplayPoint { row: 0, column: 4 }),
            Position::new(0, 1)
        );
    }

    #[test]
    fn wrap_snapshot_layout_uses_buffer_line_visual_rows_for_folded_display_rows() {
        let buffer = TextBuffer::new("zero\none\ntwo");
        let mut display_map = DisplayMap::new();
        let folded_lines = HashSet::from([0]);
        let fold_regions = vec![FoldRegion::new(0, 1, crate::FoldKind::Block)];
        display_map.sync(3, &fold_regions, &folded_lines);
        display_map.update_wrap_rows(&[0, 2], 4, &[2, 3]);

        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &fold_regions,
            &folded_lines,
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );
        let layout = snapshot
            .wrap_snapshot()
            .layout_for_rows(0..2, 0.0, gpui::px(10.0));

        assert_eq!(layout.visual_rows_for_slot(0), Some(2));
        assert_eq!(layout.visual_rows_for_slot(1), Some(3));
        assert_eq!(layout.line_y_offset_for_slot(1), Some(gpui::px(20.0)));
    }

    #[test]
    fn row_ids_stay_stable_for_unaffected_prefix_when_fold_suffix_changes() {
        let mut display_map = DisplayMap::new();
        let folded_lines = HashSet::from([3]);
        display_map.sync(
            6,
            &[FoldRegion::new(3, 4, crate::FoldKind::Block)],
            &folded_lines,
        );
        let before = display_map.snapshot(
            TextBuffer::new("a\nb\nc\nd\ne\nf").snapshot(),
            &[FoldRegion::new(3, 4, crate::FoldKind::Block)],
            &folded_lines,
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        display_map.sync_range(
            7,
            &[FoldRegion::new(4, 5, crate::FoldKind::Block)],
            &HashSet::from([4]),
            4..6,
        );
        let after = display_map.snapshot(
            TextBuffer::new("a\nb\nc\nd\ne\nf\ng").snapshot(),
            &[FoldRegion::new(4, 5, crate::FoldKind::Block)],
            &HashSet::from([4]),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(before.row_infos(0..3), after.row_infos(0..3));
    }

    #[test]
    fn visual_row_infos_expand_wrapped_lines_in_paint_order() {
        let mut display_map = DisplayMap::from_display_lines(vec![0, 2]);
        display_map.sync(3, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0, 2], 4, &[2, 3]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("zero\none\ntwo").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.visual_display_row_count(), 6);
        let visual_rows = snapshot.visual_row_infos(0..6);
        assert_eq!(visual_rows.len(), 6);
        assert_eq!(
            visual_rows[0].row_id,
            DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0
            }
        );
        assert_eq!(
            visual_rows[1].row_id,
            DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 1
            }
        );
        assert_eq!(
            visual_rows[2].row_id,
            DisplayRowId {
                buffer_line: 1,
                wrap_subrow: 0
            }
        );
        assert_eq!(
            visual_rows[3].row_id,
            DisplayRowId {
                buffer_line: 2,
                wrap_subrow: 0
            }
        );
        assert_eq!(
            visual_rows[5].row_id,
            DisplayRowId {
                buffer_line: 2,
                wrap_subrow: 2
            }
        );
    }

    #[test]
    fn visual_row_id_for_position_tracks_wrapped_subrow_index() {
        let mut display_map = DisplayMap::from_display_lines(vec![0, 1]);
        display_map.sync(2, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0, 1], 4, &[2, 2]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("abcdefgh\nijklmnop").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        let visual_row = snapshot
            .visual_row_id_for_position(Position::new(1, 6))
            .expect("visual row for wrapped position");
        assert_eq!(
            visual_row.row_id,
            DisplayRowId {
                buffer_line: 1,
                wrap_subrow: 1
            }
        );
        assert_eq!(visual_row.display_row, 1);
        assert_eq!(visual_row.visual_row, 3);
    }

    #[test]
    fn wrap_snapshot_builds_visible_layout_offsets_from_visual_rows() {
        let snapshot = test_wrap_snapshot(vec![1, 1, 1, 2, 1, 3], 12);
        let layout = snapshot.layout_for_rows(3..6, 3.5, gpui::px(10.0));

        assert_eq!(layout.wrap_column(), 12);
        assert_eq!(layout.line_y_offset_for_slot(3), Some(gpui::px(-5.0)));
        assert_eq!(layout.line_y_offset_for_slot(4), Some(gpui::px(15.0)));
        assert_eq!(layout.line_y_offset_for_slot(5), Some(gpui::px(25.0)));
        assert_eq!(layout.visual_rows_for_slot(5), Some(3));
    }

    #[test]
    fn wrap_snapshot_reuses_cached_visible_layout_for_identical_inputs() {
        let snapshot = test_wrap_snapshot(vec![1, 2, 3], 8);

        let first = snapshot.layout_for_rows(0..3, 0.0, gpui::px(10.0));
        let second = snapshot.layout_for_rows(0..3, 0.0, gpui::px(10.0));

        assert!(Arc::ptr_eq(
            &first.line_y_offsets(),
            &second.line_y_offsets()
        ));
        assert!(Arc::ptr_eq(&first.visual_rows(), &second.visual_rows()));
    }

    #[test]
    fn wrap_snapshot_invalidates_cached_visible_layout_when_wrap_inputs_change() {
        let snapshot = test_wrap_snapshot(vec![1, 2, 3], 8);

        let base = snapshot.layout_for_rows(0..3, 0.0, gpui::px(10.0));
        let resized = snapshot.layout_for_rows(0..3, 0.0, gpui::px(12.0));

        assert!(!Arc::ptr_eq(
            &base.line_y_offsets(),
            &resized.line_y_offsets()
        ));
    }

    #[test]
    fn fold_display_range_patch_matches_full_sync_after_edit() {
        let fold_regions = vec![FoldRegion::new(1, 3, crate::FoldKind::Block)];
        let mut patched = FoldDisplayState::new();
        let mut rebuilt = FoldDisplayState::new();

        patched.collapse_line(1);
        rebuilt.collapse_line(1);

        patched.sync_all(6, &fold_regions);
        rebuilt.sync_all(6, &fold_regions);

        patched.sync_range(7, &fold_regions, 4..6);
        rebuilt.sync_all(7, &fold_regions);

        assert_eq!(
            patched.display_lines().as_ref(),
            rebuilt.display_lines().as_ref()
        );
    }

    #[test]
    fn wrap_map_sync_range_shifts_unaffected_suffix_rows() {
        let mut display_map = DisplayMap::new();
        display_map.sync(4, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0, 1, 2, 3], 8, &[1, 2, 3, 4]);

        display_map.sync_range(5, &[], &HashSet::new(), 1..3);
        let snapshot = display_map.snapshot(
            TextBuffer::new("zero\none\ntwo\nthree\nfour").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(display_map.last_wrap_sync_range(), Some(1..3));
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(0), 1);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(1), 1);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(2), 1);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(3), 3);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(4), 4);
    }

    #[test]
    fn wrap_map_update_rows_replaces_cached_counts_for_visible_lines() {
        let mut display_map = DisplayMap::new();
        display_map.sync(3, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0, 1, 2], 10, &[1, 1, 1]);
        display_map.update_wrap_rows(&[1], 10, &[4]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("zero\none\ntwo").snapshot(),
            &[],
            &HashSet::new(),
            true,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.wrap_snapshot().wrap_column(), 10);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(0), 1);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(1), 4);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(2), 1);
    }

    #[test]
    fn wrap_map_update_rows_reports_changed_visible_line_range() {
        let mut display_map = DisplayMap::new();
        display_map.sync(4, &[], &HashSet::new());
        assert!(display_map.update_wrap_rows(&[0, 1, 2, 3], 8, &[1, 1, 1, 1]));
        assert_eq!(display_map.last_wrap_sync_range(), Some(0..4));

        assert!(display_map.update_wrap_rows(&[1], 8, &[3]));
        assert_eq!(display_map.last_wrap_sync_range(), Some(1..2));
        assert!(!display_map.update_wrap_rows(&[1], 8, &[3]));
    }

    #[test]
    fn wrap_snapshot_disables_visual_row_expansion_when_soft_wrap_is_off() {
        let mut display_map = DisplayMap::new();
        display_map.sync(2, &[], &HashSet::new());
        display_map.update_wrap_rows(&[0, 1], 4, &[3, 2]);

        let snapshot = display_map.snapshot(
            TextBuffer::new("abcdefghij\nshort").snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[],
        );

        assert_eq!(snapshot.wrap_snapshot().wrap_column(), 0);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(0), 1);
        assert_eq!(snapshot.wrap_snapshot().visual_rows_for_line(1), 1);
    }

    #[test]
    fn block_map_reuses_cached_indexes_when_widgets_do_not_change() {
        let display_map = DisplayMap::new();
        let buffer = TextBuffer::new("zero\none\ntwo");
        let widgets = vec![AnchoredCodeAction {
            line: 1,
            label: "Quick fix".to_string(),
        }];

        let first_snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &widgets,
        );
        let second_snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &widgets,
        );

        assert!(display_map.last_block_sync_range().is_some());
        assert!(std::ptr::eq(
            first_snapshot.block_snapshot().block_widgets().as_ptr(),
            second_snapshot.block_snapshot().block_widgets().as_ptr(),
        ));
    }

    #[test]
    fn block_map_reports_changed_line_range_when_widgets_change() {
        let display_map = DisplayMap::new();
        let buffer = TextBuffer::new("zero\none\ntwo\nthree");

        let _ = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[AnchoredCodeAction {
                line: 1,
                label: "Quick fix".to_string(),
            }],
        );

        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[
                AnchoredCodeAction {
                    line: 1,
                    label: "Quick fix".to_string(),
                },
                AnchoredCodeAction {
                    line: 3,
                    label: "Quick fix".to_string(),
                },
            ],
        );

        assert_eq!(display_map.last_block_sync_range(), Some(1..4));
        assert_eq!(snapshot.block_widgets_for_line(3)[0].label, "Quick fix");
    }

    #[test]
    fn viewport_reuses_cached_row_chunk_and_block_slices_for_identical_range() {
        let buffer = TextBuffer::new("alpha\nbeta\ngamma");
        let display_map = DisplayMap::from_display_lines(vec![0, 1, 2]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(vec![Highlight {
                start: 0,
                end: 5,
                kind: HighlightKind::Keyword,
            }]),
            &[AnchoredDiagnostic {
                range: buffer
                    .anchored_range(6..10, Bias::Left, Bias::Right)
                    .expect("diagnostic range"),
                kind: HighlightKind::Error,
            }],
            Arc::new(vec![AnchoredInlayHint {
                anchor: buffer.anchor_at(11, Bias::Right).expect("inlay anchor"),
                label: "hint".to_string(),
                side: InlayHintSide::After,
                kind: None,
                padding_left: false,
                padding_right: false,
            }]),
            &[AnchoredCodeAction {
                line: 2,
                label: "Widget".to_string(),
            }],
        );

        let first = snapshot.viewport(0..3);
        let second = snapshot.viewport(0..3);

        assert!(Arc::ptr_eq(&first.row_infos(), &second.row_infos()));
        assert!(Arc::ptr_eq(&first.text_chunks(), &second.text_chunks()));
        assert!(Arc::ptr_eq(&first.block_widgets(), &second.block_widgets()));
    }

    #[test]
    fn viewport_traversal_matches_paint_order_for_rows_chunks_and_blocks() {
        let buffer = TextBuffer::new("zero\none\ntwo\nthree");
        let display_map = DisplayMap::from_display_lines(vec![0, 2, 3]);
        let snapshot = display_map.snapshot(
            buffer.snapshot(),
            &[],
            &HashSet::new(),
            false,
            Arc::new(Vec::new()),
            &[],
            Arc::new(Vec::new()),
            &[
                AnchoredCodeAction {
                    line: 2,
                    label: "two".to_string(),
                },
                AnchoredCodeAction {
                    line: 3,
                    label: "three".to_string(),
                },
            ],
        );

        let viewport = snapshot.viewport(0..3);
        let row_lines = viewport
            .row_infos()
            .iter()
            .map(|row| row.buffer_line)
            .collect::<Vec<_>>();
        let chunk_lines = viewport
            .text_chunks()
            .iter()
            .map(|chunk| chunk.buffer_line)
            .collect::<Vec<_>>();
        let block_rows = viewport
            .block_widgets()
            .iter()
            .map(|(display_row, _)| *display_row)
            .collect::<Vec<_>>();

        assert_eq!(row_lines, vec![0, 2, 3]);
        assert_eq!(chunk_lines, vec![0, 2, 3]);
        assert_eq!(block_rows, vec![1, 2]);
    }
}
