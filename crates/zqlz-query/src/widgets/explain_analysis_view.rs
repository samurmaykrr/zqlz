use std::collections::HashMap;

use gpui::prelude::FluentBuilder;
use gpui::*;
use zqlz_analyzer::{QueryAnalysis, SeverityLevel, explain::PlanNode};
use zqlz_core::get_dialect_language_name;
use zqlz_ui::widgets::{ActiveTheme, h_flex, scroll::ScrollableElement, v_flex};

use super::results_panel::ExplainResult;

pub struct ExplainAnalysisView<'a> {
    result: &'a ExplainResult,
    analysis: &'a QueryAnalysis,
}

impl<'a> ExplainAnalysisView<'a> {
    pub fn new(result: &'a ExplainResult, analysis: &'a QueryAnalysis) -> Self {
        Self { result, analysis }
    }

    pub fn render_visual(&self, cx: &mut App) -> AnyElement {
        let theme = cx.theme();
        let max_cost = self
            .analysis
            .plan
            .iter_nodes()
            .map(Self::node_cost)
            .fold(0.0, f64::max)
            .max(1.0);
        let provider = self.provider_label();

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                h_flex()
                    .h(px(36.0))
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
                            .child(format!("provider {provider}")),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(format!("cost threshold {}%", self.cost_threshold_percent())),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .text_xs()
                            .font_family(theme.mono_font_family.clone())
                            .text_color(theme.muted_foreground)
                            .child(format!(
                                "score {}  suggestions {}",
                                self.analysis.performance_score,
                                self.analysis.suggestions.len()
                            )),
                    ),
            )
            .child(
                div().flex_1().w_full().overflow_hidden().child(
                    h_flex()
                        .size_full()
                        .p_6()
                        .items_center()
                        .justify_center()
                        .child(self.render_visual_node(&self.analysis.plan.root, max_cost, cx)),
                ),
            )
            .into_any_element()
    }

    pub fn render_suggestions(&self, cx: &mut App) -> AnyElement {
        self.render_findings_strip(cx)
    }

    pub fn render_statistics(&self, cx: &mut App) -> AnyElement {
        let theme = cx.theme();
        let total_cost = self
            .analysis
            .plan
            .total_cost
            .unwrap_or_else(|| Self::node_cost(&self.analysis.plan.root))
            .max(1.0);
        let node_rows = self.node_type_rows(total_cost);
        let relation_rows = self.relation_rows(total_cost);

        v_flex()
            .size_full()
            .bg(theme.background)
            .child(
                h_flex()
                    .h(px(36.0))
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
                            .child("Statistics"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child("Per Node Type"),
                    )
                    .child(div().flex_1())
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(self.provider_label()),
                    ),
            )
            .child(
                div().flex_1().overflow_y_scrollbar().child(
                    v_flex()
                        .w_full()
                        .child(self.render_node_type_table(&node_rows, cx))
                        .child(self.render_relation_table(&relation_rows, cx)),
                ),
            )
            .into_any_element()
    }

    fn render_visual_node(&self, node: &PlanNode, max_cost: f64, cx: &mut App) -> AnyElement {
        let child_nodes = node
            .children
            .iter()
            .map(|child| self.render_visual_node(child, max_cost, cx))
            .collect::<Vec<_>>();
        let theme = cx.theme();
        let cost = Self::node_cost(node);
        let intensity = (cost / max_cost).clamp(0.0, 1.0) as f32;
        let accent = if intensity >= 0.5 {
            theme.warning
        } else if node.is_scan() {
            theme.info
        } else {
            theme.muted_foreground
        };
        let title = Self::node_title(node);
        let subtitle = node
            .relation
            .as_ref()
            .or(node.index_name.as_ref())
            .cloned()
            .unwrap_or_else(|| Self::node_kind(node).to_string());

        h_flex()
            .gap_7()
            .items_center()
            .child(
                v_flex()
                    .w(px(132.0))
                    .h(px(74.0))
                    .p_2()
                    .gap_1()
                    .border_1()
                    .border_color(theme.border)
                    .bg(theme.muted.opacity(0.12 + (intensity * 0.16)))
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(title),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .overflow_hidden()
                            .text_ellipsis()
                            .whitespace_nowrap()
                            .child(subtitle),
                    )
                    .child(div().flex_1())
                    .child(
                        h_flex()
                            .justify_between()
                            .items_center()
                            .child(
                                div()
                                    .text_xs()
                                    .font_family(theme.mono_font_family.clone())
                                    .text_color(accent)
                                    .child(Self::format_cost(cost)),
                            )
                            .child(
                                div()
                                    .text_xs()
                                    .font_family(theme.mono_font_family.clone())
                                    .text_color(theme.muted_foreground)
                                    .child(format!(
                                        "{} rows",
                                        node.rows
                                            .map(Self::format_count)
                                            .unwrap_or_else(|| "-".to_string())
                                    )),
                            ),
                    ),
            )
            .when(!child_nodes.is_empty(), |this| {
                this.child(div().w(px(28.0)).h(px(1.0)).bg(theme.border.opacity(0.8)))
                    .child(v_flex().gap_4().children(child_nodes))
            })
            .into_any_element()
    }

    fn render_findings_strip(&self, cx: &mut App) -> AnyElement {
        let theme = cx.theme();

        v_flex()
            .size_full()
            .border_color(theme.border)
            .child(
                h_flex()
                    .h(px(28.0))
                    .px_3()
                    .gap_2()
                    .items_center()
                    .bg(theme.muted.opacity(0.16))
                    .border_b_1()
                    .border_color(theme.border)
                    .child(
                        div()
                            .text_xs()
                            .font_weight(FontWeight::SEMIBOLD)
                            .text_color(theme.foreground)
                            .child("Suggestions"),
                    )
                    .child(
                        div()
                            .text_xs()
                            .text_color(theme.muted_foreground)
                            .child(self.analysis.suggestions.len().to_string()),
                    ),
            )
            .child(div().flex_1().overflow_y_scrollbar().child(
                if self.analysis.suggestions.is_empty() {
                    div()
                        .size_full()
                        .px_3()
                        .flex()
                        .items_center()
                        .justify_center()
                        .text_sm()
                        .text_color(theme.success)
                        .child("Query plan looks optimal - no issues detected.")
                        .into_any_element()
                } else {
                    v_flex()
                        .p_3()
                        .gap_3()
                        .children(self.analysis.sorted_suggestions().iter().map(|suggestion| {
                            let severity_color = Self::severity_color(suggestion.severity, theme);
                            v_flex()
                                .w_full()
                                .gap_2()
                                .p_3()
                                .border_1()
                                .border_color(theme.border.opacity(0.8))
                                .bg(theme.muted.opacity(0.08))
                                .child(
                                    h_flex()
                                        .gap_2()
                                        .items_center()
                                        .child(
                                            div()
                                                .px_2()
                                                .py(px(2.0))
                                                .bg(severity_color.opacity(0.14))
                                                .border_1()
                                                .border_color(severity_color.opacity(0.45))
                                                .text_xs()
                                                .font_weight(FontWeight::SEMIBOLD)
                                                .text_color(severity_color)
                                                .child(suggestion.severity.as_str()),
                                        )
                                        .when_some(suggestion.table.as_ref(), |this, table| {
                                            this.child(
                                                div()
                                                    .px_2()
                                                    .py(px(2.0))
                                                    .bg(theme.background)
                                                    .border_1()
                                                    .border_color(theme.border.opacity(0.7))
                                                    .text_xs()
                                                    .font_family(theme.mono_font_family.clone())
                                                    .text_color(theme.muted_foreground)
                                                    .child(table.clone()),
                                            )
                                        }),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .font_weight(FontWeight::SEMIBOLD)
                                        .text_color(theme.foreground)
                                        .line_height(relative(1.35))
                                        .child(suggestion.message.clone()),
                                )
                                .child(
                                    div()
                                        .text_sm()
                                        .text_color(theme.muted_foreground)
                                        .line_height(relative(1.35))
                                        .child(suggestion.recommendation.clone()),
                                )
                                .when(!suggestion.columns.is_empty(), |this| {
                                    this.child(h_flex().gap_1().flex_wrap().children(
                                        suggestion.columns.iter().map(|column| {
                                            div()
                                                .px_2()
                                                .py(px(2.0))
                                                .bg(theme.background)
                                                .border_1()
                                                .border_color(theme.border.opacity(0.7))
                                                .text_xs()
                                                .font_family(theme.mono_font_family.clone())
                                                .text_color(theme.foreground)
                                                .child(column.clone())
                                        }),
                                    ))
                                })
                        }))
                        .into_any_element()
                },
            ))
            .into_any_element()
    }

    fn render_node_type_table(
        &self,
        rows: &[(String, usize, f64, f64)],
        cx: &mut App,
    ) -> AnyElement {
        let title = self.render_section_title("Per Node Type", cx);
        let header = self.render_stats_header(&["Node Type", "Count", "Cost", "Cost (%)"], cx);
        let theme = cx.theme();

        v_flex()
            .w_full()
            .flex_none()
            .child(title)
            .child(header)
            .children(rows.iter().map(|(node_type, count, cost, cost_percent)| {
                self.render_stats_row(
                    &[
                        node_type.clone(),
                        count.to_string(),
                        Self::format_cost(*cost),
                        Self::format_percent(*cost_percent),
                    ],
                    theme,
                )
            }))
            .into_any_element()
    }

    fn render_relation_table(
        &self,
        rows: &[(String, usize, f64, f64, String)],
        cx: &mut App,
    ) -> AnyElement {
        let title = self.render_section_title("Per Relation", cx);
        let header = self.render_stats_header(
            &[
                "Relation Name",
                "Scan Count",
                "Cost",
                "Cost (%)",
                "Dominant Node",
            ],
            cx,
        );
        let theme = cx.theme();

        v_flex()
            .w_full()
            .flex_none()
            .mt_4()
            .child(title)
            .child(header)
            .children(rows.iter().map(
                |(relation, scan_count, cost, cost_percent, dominant_node)| {
                    self.render_stats_row(
                        &[
                            relation.clone(),
                            scan_count.to_string(),
                            Self::format_cost(*cost),
                            Self::format_percent(*cost_percent),
                            dominant_node.clone(),
                        ],
                        theme,
                    )
                },
            ))
            .into_any_element()
    }

    fn render_section_title(&self, title: &'static str, cx: &mut App) -> AnyElement {
        let theme = cx.theme();

        div()
            .h(px(30.0))
            .px_3()
            .flex()
            .items_center()
            .text_sm()
            .font_weight(FontWeight::SEMIBOLD)
            .text_color(theme.foreground)
            .child(title)
            .into_any_element()
    }

    fn render_stats_header(&self, columns: &[&str], cx: &mut App) -> AnyElement {
        let theme = cx.theme();

        h_flex()
            .h(px(28.0))
            .flex_none()
            .px_3()
            .items_center()
            .bg(theme.muted.opacity(0.18))
            .border_y_1()
            .border_color(theme.border)
            .children(columns.iter().enumerate().map(|(index, column)| {
                div()
                    .when(index == 0, |this| this.flex_1())
                    .when(index != 0, |this| this.w(px(150.0)))
                    .text_xs()
                    .font_weight(FontWeight::SEMIBOLD)
                    .text_color(theme.foreground)
                    .child((*column).to_string())
            }))
            .into_any_element()
    }

    fn render_stats_row(&self, columns: &[String], theme: &zqlz_ui::widgets::Theme) -> AnyElement {
        h_flex()
            .h(px(30.0))
            .flex_none()
            .px_3()
            .items_center()
            .border_b_1()
            .border_color(theme.border.opacity(0.7))
            .children(columns.iter().enumerate().map(|(index, column)| {
                div()
                    .when(index == 0, |this| this.flex_1())
                    .when(index != 0, |this| this.w(px(150.0)))
                    .text_sm()
                    .text_color(if index == 0 {
                        theme.foreground
                    } else {
                        theme.muted_foreground
                    })
                    .overflow_hidden()
                    .text_ellipsis()
                    .whitespace_nowrap()
                    .child(column.clone())
            }))
            .into_any_element()
    }

    fn node_type_rows(&self, total_cost: f64) -> Vec<(String, usize, f64, f64)> {
        let mut rows: HashMap<String, (usize, f64)> = HashMap::new();
        for node in self.analysis.plan.iter_nodes() {
            let entry = rows.entry(Self::node_title(node)).or_insert((0, 0.0));
            entry.0 += 1;
            entry.1 += Self::node_cost(node);
        }

        let mut rows = rows
            .into_iter()
            .map(|(node_type, (count, cost))| (node_type, count, cost, cost / total_cost))
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b.2.total_cmp(&a.2));
        rows
    }

    fn relation_rows(&self, total_cost: f64) -> Vec<(String, usize, f64, f64, String)> {
        let mut rows: HashMap<String, (usize, f64, String)> = HashMap::new();
        for node in self.analysis.plan.iter_nodes() {
            let Some(relation) = node.relation.as_ref().or(node.alias.as_ref()).cloned() else {
                continue;
            };
            let entry = rows
                .entry(relation)
                .or_insert((0, 0.0, Self::node_title(node)));
            let node_cost = Self::node_cost(node);
            if node.is_scan() {
                entry.0 += 1;
            }
            if node_cost >= entry.1 {
                entry.2 = Self::node_title(node);
            }
            entry.1 += node_cost;
        }

        let mut rows = rows
            .into_iter()
            .map(|(relation, (scan_count, cost, dominant_node))| {
                (relation, scan_count, cost, cost / total_cost, dominant_node)
            })
            .collect::<Vec<_>>();
        rows.sort_by(|a, b| b.2.total_cmp(&a.2));
        rows
    }

    fn provider_label(&self) -> String {
        provider_label_for_result(self.result)
    }

    fn cost_threshold_percent(&self) -> usize {
        50
    }

    fn severity_color(severity: SeverityLevel, theme: &zqlz_ui::widgets::Theme) -> Hsla {
        match severity {
            SeverityLevel::Critical => theme.danger,
            SeverityLevel::Warning => theme.warning,
            SeverityLevel::Info => theme.info,
        }
    }

    fn node_cost(node: &PlanNode) -> f64 {
        node.cost.map(|cost| cost.total).unwrap_or(0.0)
    }

    fn node_title(node: &PlanNode) -> String {
        format!("{:?}", node.node_type)
            .replace("SeqScan", "Seq Scan")
            .replace("IndexScan", "Index Scan")
            .replace("IndexOnlyScan", "Index Only Scan")
            .replace("HashJoin", "Hash Join")
            .replace("NestedLoop", "Nested Loop")
            .replace("MergeJoin", "Merge Join")
            .replace("BitmapIndexScan", "Bitmap Index Scan")
            .replace("BitmapHeapScan", "Bitmap Heap Scan")
            .replace("HashAggregate", "Hash Aggregate")
            .replace("GroupAggregate", "Group Aggregate")
            .replace("IncrementalSort", "Incremental Sort")
    }

    fn node_kind(node: &PlanNode) -> &'static str {
        if node.is_scan() {
            "scan"
        } else if node.is_join() {
            "join"
        } else if node.node_type.is_potentially_slow() {
            "hot"
        } else {
            "node"
        }
    }

    fn format_cost(value: f64) -> String {
        if value >= 1_000_000.0 {
            format!("{:.1}M", value / 1_000_000.0)
        } else if value >= 1_000.0 {
            format!("{:.1}K", value / 1_000.0)
        } else {
            format!("{value:.2}")
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

    fn format_percent(value: f64) -> String {
        format!("{:.2}%", value * 100.0)
    }
}

fn provider_label_for_result(result: &ExplainResult) -> String {
    if let Some(provider_id) = result.provider_id.as_ref() {
        return get_dialect_language_name(provider_id).to_string();
    }

    let sql = result.sql.to_ascii_lowercase();
    if sql.contains("::jsonb") || sql.contains("jsonb") || sql.contains("tstz") {
        "PostgreSQL".to_string()
    } else if result.query_plan.is_some() && result.raw_output.is_some() {
        "SQLite".to_string()
    } else {
        result
            .connection_name
            .clone()
            .unwrap_or_else(|| "SQL".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::{ExplainResult, provider_label_for_result};

    fn explain_result(provider_id: Option<&str>) -> ExplainResult {
        ExplainResult {
            sql: "SELECT 1".to_string(),
            duration_ms: 0,
            duration_micros: 0,
            raw_output: None,
            query_plan: None,
            analyzed_plan: None,
            provider_id: provider_id.map(ToOwned::to_owned),
            error: None,
            connection_name: Some("fallback".to_string()),
            database_name: None,
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn provider_label_uses_core_dialect_registry() {
        assert_eq!(
            provider_label_for_result(&explain_result(Some("postgresql"))),
            "PostgreSQL"
        );
        assert_eq!(
            provider_label_for_result(&explain_result(Some("sqlserver"))),
            "Microsoft SQL Server"
        );
        assert_eq!(
            provider_label_for_result(&explain_result(Some("mongo"))),
            "MongoDB Shell"
        );
        assert_eq!(
            provider_label_for_result(&explain_result(Some("unknown"))),
            "unknown"
        );
    }
}
