#![recursion_limit = "256"]

use std::collections::HashMap;

use gpui::StatefulInteractiveElement as _;
use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_analyzer::{
    QueryAnalysis,
    explain::{JoinType, NodeType, PlanNode},
};
use zqlz_ui::widgets::{
    ActiveTheme, Icon, IconName, Sizable as _, h_flex,
    button::{Button, ButtonVariants as _},
    scroll::{ScrollableElement, Scrollbar},
    v_flex,
};

const NODE_WIDTH: f32 = 208.0;
const NODE_HEIGHT: f32 = 148.0;
const COLUMN_GAP: f32 = 292.0;
const ROW_GAP: f32 = 168.0;
const CANVAS_PADDING_X: f32 = 80.0;
const CANVAS_PADDING_Y: f32 = 72.0;

/// Share of the plan's total cost that a node must incur *by itself* before it
/// is flagged as the expensive node. Postgres total costs are cumulative, so
/// comparing cumulative cost against the root would always flag the root.
const COST_THRESHOLD: f64 = 0.2;

const MIN_ZOOM: f32 = 0.5;
const MAX_ZOOM: f32 = 2.0;
const ZOOM_STEP: f32 = 0.2;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ExplainGraphNodeId(usize);

impl ExplainGraphNodeId {
    pub fn raw(self) -> usize {
        self.0
    }
}

#[derive(Clone, Debug)]
pub enum ExplainGraphEvent {
    NodeSelected(ExplainGraphNodeId),
    NodeCleared,
}

#[derive(Clone, Debug)]
pub struct ExplainGraphInput {
    pub provider_id: Option<String>,
    pub connection_name: Option<String>,
    pub duration_ms: u64,
}

#[derive(Debug)]
pub struct ExplainGraphState {
    graph: ExplainGraph,
    provider_label: String,
    performance_score: u8,
    suggestion_count: usize,
    duration_ms: u64,
    selected_node_id: Option<ExplainGraphNodeId>,
    scroll_handle: ScrollHandle,
    zoom: f32,
}

impl ExplainGraphState {
    pub fn new(analysis: QueryAnalysis, input: ExplainGraphInput) -> Self {
        let provider_label = provider_label(input.provider_id.as_deref());
        Self {
            graph: ExplainGraph::from_analysis(&analysis),
            provider_label,
            performance_score: analysis.performance_score,
            suggestion_count: analysis.suggestions.len(),
            duration_ms: input.duration_ms,
            selected_node_id: None,
            scroll_handle: ScrollHandle::new(),
            zoom: 1.0,
        }
    }

    pub fn zoom(&self) -> f32 {
        self.zoom
    }

    pub fn zoom_in(&mut self) {
        self.zoom = (self.zoom + ZOOM_STEP).min(MAX_ZOOM);
    }

    pub fn zoom_out(&mut self) {
        self.zoom = (self.zoom - ZOOM_STEP).max(MIN_ZOOM);
    }

    pub fn reset_zoom(&mut self) {
        self.zoom = 1.0;
    }

    pub fn graph(&self) -> &ExplainGraph {
        &self.graph
    }

    pub fn select_node(&mut self, id: ExplainGraphNodeId) -> ExplainGraphEvent {
        self.selected_node_id = Some(id);
        ExplainGraphEvent::NodeSelected(id)
    }

    pub fn clear_selection(&mut self) -> ExplainGraphEvent {
        self.selected_node_id = None;
        ExplainGraphEvent::NodeCleared
    }

    pub fn selected_node_id(&self) -> Option<ExplainGraphNodeId> {
        self.selected_node_id
    }
}

pub struct ExplainGraphView {
    state: Entity<ExplainGraphState>,
}

impl ExplainGraphView {
    pub fn new(state: Entity<ExplainGraphState>) -> Self {
        Self { state }
    }

    pub fn render(&self, cx: &mut App) -> AnyElement {
        let background = cx.theme().background;
        let state = self.state.read(cx);
        let graph = state.graph.clone();
        let selected_node = state
            .selected_node_id
            .and_then(|id| graph.node(id).cloned());
        let scroll_handle = state.scroll_handle.clone();
        let zoom = state.zoom;
        let header = GraphHeader {
            provider_label: state.provider_label.clone(),
            performance_score: state.performance_score,
            suggestion_count: state.suggestion_count,
            duration_ms: state.duration_ms,
            zoom,
        };
        let graph_state = self.state.clone();

        v_flex()
            .size_full()
            .bg(background)
            .child(self.render_header(header, cx))
            .child(
                div()
                    .relative()
                    .flex_1()
                    .w_full()
                    .bg(background)
                    .child(self.render_canvas(&graph, scroll_handle, zoom, cx))
                    .when_some(selected_node, |this, node| {
                        this.child(self.render_detail_panel(node, graph_state, cx))
                    }),
            )
            .into_any_element()
    }

    fn render_header(&self, header: GraphHeader, cx: &mut App) -> AnyElement {
        let theme = cx.theme();
        let zoom_percent = (header.zoom * 100.0).round() as i32;
        let zoom_out_state = self.state.clone();
        let zoom_in_state = self.state.clone();
        let reset_state = self.state.clone();

        h_flex()
            .h(px(36.0))
            .w_full()
            .px_3()
            .gap_4()
            .items_center()
            .border_b_1()
            .border_color(theme.border)
            .child(
                div()
                    .text_sm()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child("Visual"),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!("provider {}", header.provider_label)),
            )
            .child(
                div()
                    .text_xs()
                    .text_color(theme.muted_foreground)
                    .child(format!(
                        "highlight above {:.0}% self cost",
                        COST_THRESHOLD * 100.0
                    )),
            )
            .child(
                h_flex()
                    .gap_1()
                    .items_center()
                    .child(
                        Button::new("explain-graph-zoom-out")
                            .ghost()
                            .xsmall()
                            .icon(IconName::Minus)
                            .tooltip("Zoom out")
                            .on_click(move |_, _, cx| {
                                zoom_out_state.update(cx, |state, cx| {
                                    state.zoom_out();
                                    cx.notify();
                                });
                            }),
                    )
                    .child(
                        Button::new("explain-graph-zoom-reset")
                            .ghost()
                            .xsmall()
                            .label(format!("{zoom_percent}%"))
                            .tooltip("Reset zoom to 100%")
                            .on_click(move |_, _, cx| {
                                reset_state.update(cx, |state, cx| {
                                    state.reset_zoom();
                                    cx.notify();
                                });
                            }),
                    )
                    .child(
                        Button::new("explain-graph-zoom-in")
                            .ghost()
                            .xsmall()
                            .icon(IconName::Plus)
                            .tooltip("Zoom in")
                            .on_click(move |_, _, cx| {
                                zoom_in_state.update(cx, |state, cx| {
                                    state.zoom_in();
                                    cx.notify();
                                });
                            }),
                    ),
            )
            .child(div().flex_1())
            .child(
                div()
                    .text_xs()
                    .font_family(theme.mono_font_family.clone())
                    .text_color(theme.muted_foreground)
                    .child(format!(
                        "score {}  suggestions {}  explain {}ms",
                        header.performance_score, header.suggestion_count, header.duration_ms
                    )),
            )
            .into_any_element()
    }

    fn render_canvas(
        &self,
        graph: &ExplainGraph,
        scroll_handle: ScrollHandle,
        zoom: f32,
        cx: &mut App,
    ) -> AnyElement {
        let content_width = (graph.bounds_width * zoom).max(1.0);
        let content_height = ((graph.bounds_height + 220.0) * zoom).max(1.0);
        let scrollbar_width = px(12.0);
        let state = self.state.clone();

        div()
            .relative()
            .size_full()
            .child(
                div()
                    .id("explain-graph-scroll")
                    .absolute()
                    .top_0()
                    .left_0()
                    .right(scrollbar_width)
                    .bottom(scrollbar_width)
                    .track_scroll(&scroll_handle)
                    .overflow_scroll()
                    .child(
                        div()
                            .relative()
                            .flex_none()
                            .w(px(content_width))
                            .h(px(content_height))
                            .children(
                                graph
                                    .edges
                                    .iter()
                                    .map(|edge| self.render_edge(graph, edge, zoom, cx)),
                            )
                            .children(graph.nodes.iter().map(|node| {
                                self.render_node(
                                    node,
                                    graph.root_total_cost,
                                    zoom,
                                    state.clone(),
                                    cx,
                                )
                            })),
                    ),
            )
            .child(
                div()
                    .absolute()
                    .top_0()
                    .right_0()
                    .bottom(scrollbar_width)
                    .w(scrollbar_width)
                    .child(Scrollbar::vertical(&scroll_handle)),
            )
            .child(
                div()
                    .absolute()
                    .left_0()
                    .right(scrollbar_width)
                    .bottom_0()
                    .h(scrollbar_width)
                    .child(Scrollbar::horizontal(&scroll_handle)),
            )
            .into_any_element()
    }

    fn render_edge(
        &self,
        graph: &ExplainGraph,
        edge: &GraphEdge,
        zoom: f32,
        cx: &mut App,
    ) -> AnyElement {
        let Some(parent) = graph.node(edge.parent_id) else {
            return div().into_any_element();
        };
        let Some(child) = graph.node(edge.child_id) else {
            return div().into_any_element();
        };
        let theme = cx.theme();
        let start_x = (parent.x + NODE_WIDTH) * zoom;
        let start_y = (parent.y + NODE_HEIGHT / 2.0) * zoom;
        let end_x = child.x * zoom;
        let end_y = (child.y + NODE_HEIGHT / 2.0) * zoom;
        let middle_x = start_x + ((end_x - start_x) / 2.0).max(34.0 * zoom);
        let horizontal_width = (middle_x - start_x).max(1.0);
        let trailing_width = (end_x - middle_x).max(1.0);
        let vertical_top = start_y.min(end_y);
        let vertical_height = (start_y - end_y).abs().max(1.0);

        div()
            .absolute()
            .left(px(0.0))
            .top(px(0.0))
            .child(
                div()
                    .absolute()
                    .left(px(start_x))
                    .top(px(start_y))
                    .w(px(horizontal_width))
                    .h(px(1.0))
                    .bg(theme.info.opacity(0.65)),
            )
            .child(
                div()
                    .absolute()
                    .left(px(middle_x))
                    .top(px(vertical_top))
                    .w(px(1.0))
                    .h(px(vertical_height))
                    .bg(theme.info.opacity(0.65)),
            )
            .child(
                div()
                    .absolute()
                    .left(px(middle_x))
                    .top(px(end_y))
                    .w(px(trailing_width))
                    .h(px(1.0))
                    .bg(theme.info.opacity(0.65)),
            )
            .into_any_element()
    }

    fn render_node(
        &self,
        node: &GraphNode,
        root_total_cost: f64,
        zoom: f32,
        state: Entity<ExplainGraphState>,
        cx: &mut App,
    ) -> AnyElement {
        let theme = cx.theme();
        let highlighted = node.exceeds_threshold(root_total_cost);
        let color = if highlighted {
            theme.warning
        } else {
            match node.icon_family {
                IconFamily::Scan | IconFamily::IndexScan => theme.info,
                IconFamily::Join | IconFamily::Hash => theme.success,
                _ => theme.muted_foreground,
            }
        };
        let id = node.id;
        let secondary_label = node.secondary_label();
        let rows_label = node.rows_label();
        let filtered_label = node
            .rows_removed_by_filter
            .map(|removed| format!("{} filtered out", format_count(removed)));

        v_flex()
            .absolute()
            .left(px(node.x * zoom))
            .top(px(node.y * zoom))
            .w(px(NODE_WIDTH * zoom))
            .h(px(NODE_HEIGHT * zoom))
            .px(px(6.0 * zoom))
            .items_center()
            .gap(px(3.0 * zoom))
            .cursor_pointer()
            .on_mouse_down(MouseButton::Left, move |_, _, cx| {
                state.update(cx, |state, cx| {
                    state.select_node(id);
                    cx.notify();
                });
            })
            .child(
                div()
                    .text_size(px(12.0 * zoom))
                    .font_family(theme.mono_font_family.clone())
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(color)
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(
                        node.total_cost
                            .map(|total_cost| format!("cost {}", format_cost(total_cost)))
                            .unwrap_or_else(|| node.operation_label.clone()),
                    ),
            )
            .child(
                h_flex()
                    .w(px(56.0 * zoom))
                    .h(px(36.0 * zoom))
                    .items_center()
                    .justify_center()
                    .border_1()
                    .border_color(if highlighted {
                        theme.warning
                    } else {
                        theme.border
                    })
                    .bg(theme.muted.opacity(0.20))
                    .child(
                        Icon::new(node.icon_family.icon_name())
                            .size(px(24.0 * zoom))
                            .text_color(color),
                    ),
            )
            .child(
                div()
                    .w_full()
                    .text_center()
                    .text_size(px(15.0 * zoom))
                    .font_weight(FontWeight::BOLD)
                    .text_color(theme.foreground)
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(node.short_name.clone()),
            )
            .when_some(secondary_label, |this, label| {
                this.child(
                    div()
                        .w_full()
                        .text_center()
                        .text_size(px(13.0 * zoom))
                        .text_color(theme.foreground.opacity(0.8))
                        .overflow_hidden()
                        .text_ellipsis()
                        .whitespace_nowrap()
                        .child(label),
                )
            })
            .when_some(rows_label, |this, label| {
                this.child(
                    div()
                        .w_full()
                        .text_center()
                        .text_size(px(12.0 * zoom))
                        .font_family(theme.mono_font_family.clone())
                        .text_color(theme.muted_foreground)
                        .overflow_hidden()
                        .text_ellipsis()
                        .whitespace_nowrap()
                        .child(label),
                )
            })
            .when_some(filtered_label, |this, label| {
                this.child(
                    div()
                        .w_full()
                        .text_center()
                        .text_size(px(12.0 * zoom))
                        .font_family(theme.mono_font_family.clone())
                        .text_color(theme.warning)
                        .overflow_hidden()
                        .text_ellipsis()
                        .whitespace_nowrap()
                        .child(label),
                )
            })
            .into_any_element()
    }

    fn render_detail_panel(
        &self,
        node: GraphNode,
        state: Entity<ExplainGraphState>,
        cx: &mut App,
    ) -> AnyElement {
        let theme = cx.theme();
        let details = node.detail_rows();
        let copy_payload = node.copy_payload();
        let close_state = state.clone();

        v_flex()
            .absolute()
            .top(px(18.0))
            .right(px(18.0))
            .w(px(380.0))
            .max_h(px(520.0))
            .overflow_hidden()
            .border_1()
            .border_color(theme.border)
            .bg(theme.background)
            .shadow_lg()
            .child(
                h_flex()
                    .flex_none()
                    .min_h(px(44.0))
                    .max_h(px(76.0))
                    .px_3()
                    .py_2()
                    .gap_2()
                    .items_start()
                    .overflow_hidden()
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .flex_1()
                            .min_w_0()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .line_height(relative(1.25))
                            .whitespace_normal()
                            .child(node.short_name.clone()),
                    )
                    .child(div().flex_none().w(px(1.0)).h(px(18.0)).bg(theme.border))
                    .child(
                        Button::new("explain-graph-copy-all")
                            .ghost()
                            .small()
                            .icon(IconName::Copy)
                            .label("Copy All")
                            .tooltip("Copy every field of this node")
                            .on_click(move |_, _, cx| {
                                cx.write_to_clipboard(ClipboardItem::new_string(
                                    copy_payload.clone(),
                                ));
                            }),
                    )
                    .child(
                        Button::new("explain-graph-close-detail")
                            .ghost()
                            .small()
                            .icon(IconName::Close)
                            .tooltip("Close")
                            .on_click(move |_, _, cx| {
                                close_state.update(cx, |state, cx| {
                                    state.clear_selection();
                                    cx.notify();
                                });
                            }),
                    ),
            )
            .child(
                div().flex_1().min_h_0().overflow_hidden().child(
                    v_flex()
                        .size_full()
                        .p_4()
                        .gap_2()
                        .overflow_y_scrollbar()
                        .children(details.into_iter().map(|(label, value)| {
                            h_flex()
                                .w_full()
                                .gap_3()
                                .items_start()
                                .child(
                                    div()
                                        .flex_none()
                                        .w(px(150.0))
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .text_align(TextAlign::Left)
                                        .line_height(relative(1.25))
                                        .whitespace_normal()
                                        .child(format!("{label}:")),
                                )
                                .child(
                                    div()
                                        .flex_1()
                                        .min_w_0()
                                        .text_sm()
                                        .font_family(theme.mono_font_family.clone())
                                        .text_color(theme.foreground)
                                        .text_align(TextAlign::Right)
                                        .line_height(relative(1.25))
                                        .whitespace_normal()
                                        .child(value),
                                )
                        })),
                ),
            )
            .into_any_element()
    }
}

#[derive(Clone)]
struct GraphHeader {
    provider_label: String,
    performance_score: u8,
    suggestion_count: usize,
    duration_ms: u64,
    zoom: f32,
}

#[derive(Clone, Debug)]
pub struct ExplainGraph {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
    root_total_cost: f64,
    bounds_width: f32,
    bounds_height: f32,
}

impl ExplainGraph {
    pub fn from_analysis(analysis: &QueryAnalysis) -> Self {
        let mut builder = GraphBuilder::default();
        let root_id = builder.push_node(&analysis.plan.root, 0);
        let root_total_cost = node_total_cost(&analysis.plan.root).unwrap_or(1.0).max(1.0);
        let mut graph = builder.finish(root_id, root_total_cost);
        graph.layout();
        graph
    }

    pub fn nodes(&self) -> &[GraphNode] {
        &self.nodes
    }

    pub fn edges(&self) -> &[GraphEdge] {
        &self.edges
    }

    pub fn root_total_cost(&self) -> f64 {
        self.root_total_cost
    }

    pub fn node(&self, id: ExplainGraphNodeId) -> Option<&GraphNode> {
        self.nodes.iter().find(|node| node.id == id)
    }

    fn layout(&mut self) {
        let mut next_leaf = 0usize;
        self.layout_node(ExplainGraphNodeId(0), &mut next_leaf);
        let max_x = self.nodes.iter().map(|node| node.x).fold(0.0, f32::max);
        let max_y = self.nodes.iter().map(|node| node.y).fold(0.0, f32::max);
        self.bounds_width = max_x + NODE_WIDTH + CANVAS_PADDING_X;
        self.bounds_height = max_y + NODE_HEIGHT + CANVAS_PADDING_Y;
    }

    fn layout_node(&mut self, id: ExplainGraphNodeId, next_leaf: &mut usize) -> f32 {
        let Some(index) = self.nodes.iter().position(|node| node.id == id) else {
            return 0.0;
        };
        let child_ids = self.nodes[index].child_ids.clone();
        let depth = self.nodes[index].depth;
        let y = if child_ids.is_empty() {
            let y = CANVAS_PADDING_Y + (*next_leaf as f32 * ROW_GAP);
            *next_leaf += 1;
            y
        } else {
            let child_positions = child_ids
                .into_iter()
                .map(|child_id| self.layout_node(child_id, next_leaf))
                .collect::<Vec<_>>();
            let first = child_positions.first().copied().unwrap_or(CANVAS_PADDING_Y);
            let last = child_positions.last().copied().unwrap_or(first);
            (first + last) / 2.0
        };
        self.nodes[index].x = CANVAS_PADDING_X + (depth as f32 * COLUMN_GAP);
        self.nodes[index].y = y;
        y
    }
}

#[derive(Clone, Debug)]
pub struct GraphNode {
    id: ExplainGraphNodeId,
    child_ids: Vec<ExplainGraphNodeId>,
    depth: usize,
    x: f32,
    y: f32,
    short_name: String,
    node_type_description: String,
    display_label: String,
    icon_family: IconFamily,
    startup_cost: Option<f64>,
    total_cost: Option<f64>,
    self_cost: Option<f64>,
    operation_label: String,
    rows: Option<u64>,
    rows_removed_by_filter: Option<u64>,
    width: Option<u32>,
    relation: Option<String>,
    schema: Option<String>,
    alias: Option<String>,
    index_name: Option<String>,
    index_cond: Option<String>,
    join_type: Option<JoinType>,
    join_cond: Option<String>,
    filter: Option<String>,
    sort_keys: Vec<String>,
    actual_time_ms: Option<String>,
    actual_rows: Option<u64>,
    loops: Option<u64>,
    extra: HashMap<String, serde_json::Value>,
}

impl GraphNode {
    pub fn id(&self) -> ExplainGraphNodeId {
        self.id
    }

    pub fn position(&self) -> (f32, f32) {
        (self.x, self.y)
    }

    pub fn icon_family(&self) -> IconFamily {
        self.icon_family
    }

    pub fn display_label(&self) -> &str {
        &self.display_label
    }

    pub fn short_name(&self) -> &str {
        &self.short_name
    }

    /// The label shown beneath the node type: the relation, index or other
    /// subject the operation acts on. `None` when it would just repeat the
    /// node type.
    pub fn secondary_label(&self) -> Option<String> {
        if self.display_label.is_empty() || self.display_label == self.short_name {
            None
        } else {
            Some(self.display_label.clone())
        }
    }

    /// The row line shown on the node: estimate alone before EXPLAIN ANALYZE,
    /// estimate versus actual once real counts are available.
    pub fn rows_label(&self) -> Option<String> {
        match (self.rows, self.actual_rows) {
            (Some(rows), Some(actual_rows)) => Some(format!(
                "est {} / act {}",
                format_count(rows),
                format_count(actual_rows)
            )),
            (Some(rows), None) => Some(format!("{} rows", format_count(rows))),
            (None, Some(actual_rows)) => {
                Some(format!("{} rows actual", format_count(actual_rows)))
            }
            (None, None) => None,
        }
    }

    /// Share of the plan's total cost that is cumulative through this node.
    /// The root is always 1.0 because Postgres costs include all children.
    pub fn cost_percent(&self, root_total_cost: f64) -> f64 {
        let Some(total_cost) = self.total_cost else {
            return 0.0;
        };

        if root_total_cost <= 0.0 {
            0.0
        } else {
            (total_cost / root_total_cost).clamp(0.0, 1.0)
        }
    }

    /// Share of the plan's total cost incurred by this node alone, excluding
    /// the cost its children already account for.
    pub fn self_cost_percent(&self, root_total_cost: f64) -> f64 {
        let Some(self_cost) = self.self_cost else {
            return 0.0;
        };

        if root_total_cost <= 0.0 {
            0.0
        } else {
            (self_cost / root_total_cost).clamp(0.0, 1.0)
        }
    }

    pub fn exceeds_threshold(&self, root_total_cost: f64) -> bool {
        self.self_cost.is_some() && self.self_cost_percent(root_total_cost) >= COST_THRESHOLD
    }

    pub fn copy_payload(&self) -> String {
        self.detail_rows()
            .into_iter()
            .map(|(label, value)| format!("{label}: {value}"))
            .collect::<Vec<_>>()
            .join("\n")
    }

    pub fn detail_rows(&self) -> Vec<(String, String)> {
        let mut rows = Vec::new();
        rows.push(("Node Type".to_string(), self.short_name.clone()));
        rows.push((
            "Description".to_string(),
            self.node_type_description.clone(),
        ));
        rows.push(("Operation".to_string(), self.operation_label.clone()));
        if let Some(startup_cost) = self.startup_cost {
            rows.push(("Startup Cost".to_string(), format_cost(startup_cost)));
        }
        if let Some(total_cost) = self.total_cost {
            rows.push(("Total Cost".to_string(), format_cost(total_cost)));
        }
        if let Some(self_cost) = self.self_cost {
            rows.push(("Self Cost".to_string(), format_cost(self_cost)));
        }
        if let Some(plan_rows) = self.rows {
            rows.push(("Plan Rows".to_string(), format_count(plan_rows)));
        }
        if let Some(width) = self.width {
            rows.push(("Plan Width".to_string(), width.to_string()));
        }
        push_optional(&mut rows, "Relation", self.relation.as_deref());
        push_optional(&mut rows, "Schema", self.schema.as_deref());
        push_optional(&mut rows, "Alias", self.alias.as_deref());
        push_optional(&mut rows, "Index", self.index_name.as_deref());
        push_optional(&mut rows, "Index Cond", self.index_cond.as_deref());
        if let Some(join_type) = self.join_type {
            rows.push(("Join Type".to_string(), format!("{join_type:?}")));
        }
        push_optional(&mut rows, "Join Cond", self.join_cond.as_deref());
        push_optional(&mut rows, "Filter", self.filter.as_deref());
        if let Some(removed) = self.rows_removed_by_filter {
            rows.push((
                "Rows Removed by Filter".to_string(),
                format_count(removed),
            ));
        }
        if !self.sort_keys.is_empty() {
            rows.push(("Sort Keys".to_string(), self.sort_keys.join(", ")));
        }
        push_optional(&mut rows, "Actual Time", self.actual_time_ms.as_deref());
        if let Some(actual_rows) = self.actual_rows {
            rows.push(("Actual Rows".to_string(), format_count(actual_rows)));
        }
        if let Some(loops) = self.loops {
            rows.push(("Loops".to_string(), format_count(loops)));
        }
        if !self.extra.is_empty() {
            rows.push((
                "Provider Extra".to_string(),
                serde_json::to_string_pretty(&self.extra).unwrap_or_else(|_| "{}".to_string()),
            ));
        }
        rows
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IconFamily {
    Sort,
    Join,
    Hash,
    Scan,
    IndexScan,
    Append,
    Aggregate,
    Other,
}

impl IconFamily {
    fn icon_name(self) -> IconName {
        match self {
            Self::Sort => IconName::SortDescending,
            Self::Join => IconName::Network,
            Self::Hash => IconName::Hash,
            Self::Scan => IconName::Table,
            Self::IndexScan => IconName::Key,
            Self::Append => IconName::TreeStructure,
            Self::Aggregate => IconName::ChartPie,
            Self::Other => IconName::Cpu,
        }
    }
}

#[derive(Clone, Debug)]
pub struct GraphEdge {
    parent_id: ExplainGraphNodeId,
    child_id: ExplainGraphNodeId,
}

impl GraphEdge {
    pub fn parent_id(&self) -> ExplainGraphNodeId {
        self.parent_id
    }

    pub fn child_id(&self) -> ExplainGraphNodeId {
        self.child_id
    }
}

#[derive(Default)]
struct GraphBuilder {
    nodes: Vec<GraphNode>,
    edges: Vec<GraphEdge>,
}

impl GraphBuilder {
    fn push_node(&mut self, node: &PlanNode, depth: usize) -> ExplainGraphNodeId {
        let id = ExplainGraphNodeId(self.nodes.len());
        let graph_node = GraphNode {
            id,
            child_ids: Vec::new(),
            depth,
            x: 0.0,
            y: 0.0,
            short_name: node.node_type.short_name().to_string(),
            node_type_description: node.node_type.description().to_string(),
            display_label: node_display_label(node),
            icon_family: icon_family(node),
            startup_cost: node.cost.map(|cost| cost.startup),
            total_cost: node_total_cost(node),
            self_cost: node_self_cost(node),
            operation_label: node_operation_label(node),
            rows: node.rows,
            rows_removed_by_filter: node.rows_removed_by_filter,
            width: node.width,
            relation: node.relation.clone(),
            schema: node.schema.clone(),
            alias: node.alias.clone(),
            index_name: node.index_name.clone(),
            index_cond: node.index_cond.clone(),
            join_type: node.join_type,
            join_cond: node.join_cond.clone(),
            filter: node.filter.clone(),
            sort_keys: node.sort_keys.clone(),
            actual_time_ms: node.actual_time_ms.as_ref().map(|time| {
                format!(
                    "{}..{} ms",
                    format_cost(time.startup),
                    format_cost(time.total)
                )
            }),
            actual_rows: node.actual_rows,
            loops: node.loops,
            extra: node.extra.clone(),
        };
        self.nodes.push(graph_node);

        let child_ids = node
            .children
            .iter()
            .map(|child| {
                let child_id = self.push_node(child, depth + 1);
                self.edges.push(GraphEdge {
                    parent_id: id,
                    child_id,
                });
                child_id
            })
            .collect::<Vec<_>>();

        if let Some(graph_node) = self.nodes.iter_mut().find(|graph_node| graph_node.id == id) {
            graph_node.child_ids = child_ids;
        }

        id
    }

    fn finish(self, _root_id: ExplainGraphNodeId, root_total_cost: f64) -> ExplainGraph {
        ExplainGraph {
            nodes: self.nodes,
            edges: self.edges,
            root_total_cost,
            bounds_width: 0.0,
            bounds_height: 0.0,
        }
    }
}

pub fn provider_label(provider_id: Option<&str>) -> String {
    match provider_id
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "postgres" | "postgresql" | "pgsql" => "PostgreSQL".to_string(),
        "sqlite" | "sqlite3" => "SQLite".to_string(),
        "mysql" => "MySQL".to_string(),
        "mssql" | "sqlserver" => "SQL Server".to_string(),
        "duckdb" => "DuckDB".to_string(),
        "" => "Unknown".to_string(),
        other => other.to_string(),
    }
}

pub fn icon_family(node: &PlanNode) -> IconFamily {
    match node.node_type {
        NodeType::Sort => IconFamily::Sort,
        NodeType::NestedLoop | NodeType::HashJoin | NodeType::MergeJoin => IconFamily::Join,
        NodeType::Hash => IconFamily::Hash,
        NodeType::IndexScan | NodeType::IndexOnlyScan | NodeType::BitmapIndexScan => {
            IconFamily::IndexScan
        }
        NodeType::SeqScan | NodeType::BitmapHeapScan => IconFamily::Scan,
        NodeType::Append => IconFamily::Append,
        NodeType::Aggregate | NodeType::GroupAggregate | NodeType::HashAggregate => {
            IconFamily::Aggregate
        }
        _ => IconFamily::Other,
    }
}

fn node_display_label(node: &PlanNode) -> String {
    if let Some(description) = node
        .description
        .as_ref()
        .filter(|description| !description.is_empty())
    {
        return description.clone();
    }

    node.relation
        .as_ref()
        .or(node.index_name.as_ref())
        .cloned()
        .unwrap_or_else(|| node.node_type.short_name().to_string())
}

fn node_operation_label(node: &PlanNode) -> String {
    match node.node_type {
        NodeType::SeqScan => "SCAN".to_string(),
        NodeType::IndexScan | NodeType::IndexOnlyScan | NodeType::BitmapIndexScan => {
            "SEARCH".to_string()
        }
        NodeType::Sort => "SORT".to_string(),
        NodeType::NestedLoop | NodeType::HashJoin | NodeType::MergeJoin => "JOIN".to_string(),
        NodeType::HashAggregate | NodeType::Aggregate | NodeType::GroupAggregate => {
            "AGGREGATE".to_string()
        }
        NodeType::Append | NodeType::SetOp => "SET".to_string(),
        _ => node.node_type.description().to_string(),
    }
}

fn node_total_cost(node: &PlanNode) -> Option<f64> {
    node.cost.as_ref().map(|cost| cost.total)
}

/// Postgres total costs are cumulative, so a node's own contribution is its
/// total minus the totals of its children.
fn node_self_cost(node: &PlanNode) -> Option<f64> {
    let total = node_total_cost(node)?;
    let children_total = node
        .children
        .iter()
        .filter_map(node_total_cost)
        .sum::<f64>();
    Some((total - children_total).max(0.0))
}

fn push_optional(rows: &mut Vec<(String, String)>, label: &str, value: Option<&str>) {
    if let Some(value) = value.filter(|value| !value.is_empty()) {
        rows.push((label.to_string(), value.to_string()));
    }
}

fn format_cost(value: f64) -> String {
    if value >= 100.0 {
        format!("{value:.0}")
    } else if value >= 10.0 {
        format!("{value:.2}")
    } else {
        format!("{value:.3}")
    }
}

fn format_count(value: u64) -> String {
    if value >= 1_000_000 {
        format!("{:.1}M", value as f64 / 1_000_000.0)
    } else if value >= 1_000 {
        format!("{:.1}K", value as f64 / 1_000.0)
    } else {
        value.to_string()
    }
}
