use serde::Serialize;
use std::collections::HashSet;
use std::time::Instant;
use std::{env, fs, io::Read, sync::Arc};
#[cfg(feature = "syntax-probe-drivers")]
use zqlz_core::driver_document_symbols_for_capabilities;
#[cfg(feature = "syntax-probe-drivers")]
use zqlz_core::get_syntax_term_profile;
use zqlz_core::normalize_syntax_profile;
use zqlz_core::{
    SqlDocumentSymbol, driver_document_symbols, execution_unit_for_capabilities,
    get_syntax_driver_capabilities, markdown_fence_language_for_capabilities,
    syntax_bracket_pairs_for_profile, syntax_bracket_scan_mode_for_profile,
};
#[cfg(feature = "syntax-probe-drivers")]
use zqlz_core::{syntax_bracket_pairs_for_capabilities, syntax_bracket_scan_mode_for_capabilities};
#[cfg(feature = "syntax-probe-drivers")]
use zqlz_drivers::{DriverRegistry, get_syntax_metadata_for_driver};
use zqlz_text_editor::syntax::{
    Highlight, HighlightCoverage, HighlightKind, SyntaxHighlightPhaseTiming, SyntaxHighlighter,
    highlight_coverage, render_highlight_runs, syntax_quality_fixtures,
};
use zqlz_text_editor::{
    Cursor, DisplayMap, DisplayTextChunk, Selection, SelectionsCollection, SqlCompletionProvider,
    TextBuffer,
    editor_core::{EditorCoreSnapshot, LinePrefixEditMode, TextReplacementEdit},
};
#[cfg(feature = "syntax-probe-drivers")]
use zqlz_text_editor::{FoldKind, detect_folds_with_block_comments, detect_folds_with_rules};

fn main() -> anyhow::Result<()> {
    let mut profile = "sql".to_string();
    let mut driver = None;
    let mut text = None;
    let mut generated_large_sql = None;
    let mut fixture = None;
    let mut driver_fixture = None;
    let mut output_json = false;
    let mut outline = false;
    let mut brackets = false;
    let mut capabilities = false;
    let mut render_runs = false;
    let mut full_rope = false;
    let mut execution_unit = false;
    let mut editor_op = None;
    let mut bracket_cursor = None;
    let mut cursor_offset = None;
    let mut selection_start = None;
    let mut selection_end = None;
    let mut summary = false;
    let mut show_coverage = false;
    let mut fail_under = None;
    let mut repeat = 1usize;
    let mut show_timing = false;
    let mut show_phase_timing = false;
    let mut fail_avg_ms = None;
    let mut fail_phase_avg_ms = None;
    let mut fail_render_avg_ms = None;
    let mut viewport_start = None;
    let mut viewport_bytes = None;
    let mut expected_assignments = Vec::new();
    let mut rejected_assignments = Vec::new();
    let mut expected_render_assignments = Vec::new();
    let mut rejected_render_assignments = Vec::new();
    let mut token_reports = Vec::new();
    let mut expected_symbols = Vec::new();
    let mut expected_edits = Vec::new();
    let mut expected_capabilities = Vec::new();
    let mut expected_unit = None;
    let mut expected_unit_range = None;
    let mut args = env::args().skip(1);

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" => output_json = true,
            "--outline" => outline = true,
            "--brackets" => brackets = true,
            "--capabilities" => capabilities = true,
            "--render-runs" => render_runs = true,
            "--full-rope" => full_rope = true,
            "--execution-unit" => execution_unit = true,
            "--editor-op" => {
                editor_op = Some(parse_editor_op(&args.next().ok_or_else(|| {
                    anyhow::anyhow!(
                        "--editor-op needs toggle-comment|toggle-block-comment|newline|auto-close"
                    )
                })?)?);
            }
            "--summary" => summary = true,
            "--coverage" => show_coverage = true,
            "--timing" => show_timing = true,
            "--phase-timing" => {
                show_timing = true;
                show_phase_timing = true;
            }
            "--viewport-start" => {
                viewport_start = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--viewport-start needs a byte offset"))?
                        .parse::<usize>()?,
                );
            }
            "--viewport-bytes" => {
                let bytes = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--viewport-bytes needs a byte length"))?
                    .parse::<usize>()?;
                if bytes == 0 {
                    return Err(anyhow::anyhow!("--viewport-bytes must be greater than 0"));
                }
                viewport_bytes = Some(bytes);
            }
            "--bracket-cursor" => {
                bracket_cursor = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--bracket-cursor needs a byte offset"))?
                        .parse::<usize>()?,
                );
                brackets = true;
            }
            "--cursor" => {
                cursor_offset = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--cursor needs a byte offset"))?
                        .parse::<usize>()?,
                );
            }
            "--selection-start" => {
                selection_start = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--selection-start needs a byte offset"))?
                        .parse::<usize>()?,
                );
            }
            "--selection-end" => {
                selection_end = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--selection-end needs a byte offset"))?
                        .parse::<usize>()?,
                );
            }
            "--expect" => {
                expected_assignments.push(parse_assignment_arg(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--expect needs TOKEN:KIND"))?,
                )?);
            }
            "--reject" => {
                rejected_assignments.push(parse_assignment_arg(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--reject needs TOKEN:KIND"))?,
                )?);
            }
            "--expect-render" => {
                expected_render_assignments.push(parse_assignment_arg(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--expect-render needs TOKEN:KIND"))?,
                )?);
                render_runs = true;
            }
            "--reject-render" => {
                rejected_render_assignments.push(parse_assignment_arg(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--reject-render needs TOKEN:KIND"))?,
                )?);
                render_runs = true;
            }
            "--token-report" => {
                token_reports.push(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--token-report needs a token"))?,
                );
                render_runs = true;
            }
            "--expect-symbol" => {
                expected_symbols.push(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--expect-symbol needs a label"))?,
                );
            }
            "--expect-edit" => {
                expected_edits.push(parse_expected_edit(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--expect-edit needs START:END:TEXT"))?,
                )?);
            }
            "--expect-capability" => {
                expected_capabilities.push(parse_expected_capability(
                    &args
                        .next()
                        .ok_or_else(|| anyhow::anyhow!("--expect-capability needs KEY=VALUE"))?,
                )?);
            }
            "--expect-unit" => {
                expected_unit = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--expect-unit needs text"))?,
                );
            }
            "--expect-unit-range" => {
                expected_unit_range =
                    Some(parse_byte_range_arg(&args.next().ok_or_else(|| {
                        anyhow::anyhow!("--expect-unit-range needs START:END")
                    })?)?);
            }
            "--repeat" => {
                repeat = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--repeat needs a positive integer"))?
                    .parse::<usize>()?;
                if repeat == 0 {
                    return Err(anyhow::anyhow!("--repeat must be greater than 0"));
                }
                show_timing = true;
            }
            "--fail-avg-ms" => {
                let threshold = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--fail-avg-ms needs a millisecond value"))?
                    .parse::<f64>()?;
                if threshold < 0.0 {
                    return Err(anyhow::anyhow!("--fail-avg-ms must be non-negative"));
                }
                fail_avg_ms = Some(threshold);
                show_timing = true;
            }
            "--fail-phase-avg-ms" => {
                let threshold = args
                    .next()
                    .ok_or_else(|| {
                        anyhow::anyhow!("--fail-phase-avg-ms needs a millisecond value")
                    })?
                    .parse::<f64>()?;
                if threshold < 0.0 {
                    return Err(anyhow::anyhow!("--fail-phase-avg-ms must be non-negative"));
                }
                fail_phase_avg_ms = Some(threshold);
                show_timing = true;
                show_phase_timing = true;
            }
            "--fail-render-avg-ms" => {
                let threshold = args
                    .next()
                    .ok_or_else(|| {
                        anyhow::anyhow!("--fail-render-avg-ms needs a millisecond value")
                    })?
                    .parse::<f64>()?;
                if threshold < 0.0 {
                    return Err(anyhow::anyhow!("--fail-render-avg-ms must be non-negative"));
                }
                fail_render_avg_ms = Some(threshold);
                show_timing = true;
                render_runs = true;
            }
            "--fail-under" => {
                let threshold = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--fail-under needs a percent value"))?
                    .parse::<f64>()?;
                if !(0.0..=100.0).contains(&threshold) {
                    return Err(anyhow::anyhow!("--fail-under must be between 0 and 100"));
                }
                show_coverage = true;
                fail_under = Some(threshold);
            }
            "--profile" => {
                profile = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--profile needs a value"))?;
            }
            "--driver" => {
                driver = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--driver needs a value"))?,
                );
            }
            "--text" => {
                text = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--text needs a value"))?,
                );
            }
            "--file" => {
                let path = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--file needs a value"))?;
                text = Some(fs::read_to_string(path)?);
            }
            "--generated-large-sql" => {
                let statements = args
                    .next()
                    .ok_or_else(|| anyhow::anyhow!("--generated-large-sql needs statement count"))?
                    .parse::<usize>()?;
                if statements == 0 {
                    return Err(anyhow::anyhow!(
                        "--generated-large-sql must be greater than 0"
                    ));
                }
                generated_large_sql = Some(statements);
                show_coverage = true;
            }
            "--fixture" => {
                fixture = Some(
                    args.next()
                        .ok_or_else(|| anyhow::anyhow!("--fixture needs a name or `all`"))?,
                );
                show_coverage = true;
            }
            "--driver-fixture" => {
                driver_fixture =
                    Some(args.next().ok_or_else(|| {
                        anyhow::anyhow!("--driver-fixture needs a driver or `all`")
                    })?);
                show_coverage = true;
            }
            "--help" | "-h" => {
                print_help();
                return Ok(());
            }
            other => return Err(anyhow::anyhow!("unknown argument: {other}")),
        }
    }

    if let Some(fixture) = fixture {
        return run_fixture_probe(
            &fixture,
            FixtureProbeOptions {
                output_json,
                fail_under,
                repeat,
                show_timing,
                fail_avg_ms,
                fail_phase_avg_ms,
                fail_render_avg_ms,
                full_rope,
            },
        );
    }
    if let Some(driver_fixture) = driver_fixture {
        return run_driver_fixture_probe(
            &driver_fixture,
            FixtureProbeOptions {
                output_json,
                fail_under,
                repeat,
                show_timing,
                fail_avg_ms,
                fail_phase_avg_ms,
                fail_render_avg_ms,
                full_rope,
            },
        );
    }

    let requested_profile = match driver.as_deref() {
        Some(driver) => profile_for_driver(driver)?,
        None => normalize_syntax_profile(&profile),
    };

    if capabilities {
        return run_capabilities_probe(
            requested_profile,
            driver.as_deref(),
            output_json,
            summary,
            &expected_capabilities,
        );
    }

    let text = match (text, generated_large_sql) {
        (Some(_), Some(_)) => {
            return Err(anyhow::anyhow!(
                "--generated-large-sql cannot be combined with --text or --file"
            ));
        }
        (None, Some(statements)) => generated_large_sql_fixture(statements),
        (Some(text), None) => text,
        (None, None) => {
            let mut stdin = String::new();
            std::io::stdin().read_to_string(&mut stdin)?;
            stdin
        }
    };

    if outline {
        return run_outline_probe(
            &text,
            requested_profile,
            output_json,
            summary,
            &expected_symbols,
        );
    }

    let mut highlighter = SyntaxHighlighter::new().map_err(anyhow::Error::msg)?;
    highlighter.set_language_profile(requested_profile);
    if let Some(driver) = driver.as_deref() {
        apply_driver_syntax_metadata(&mut highlighter, driver)?;
    }
    if brackets {
        let cursor_offset = bracket_cursor.unwrap_or_else(|| text.len().saturating_sub(1));
        return run_bracket_probe(
            &text,
            requested_profile,
            cursor_offset,
            output_json,
            summary,
        );
    }
    if execution_unit {
        let cursor_offset = cursor_offset.unwrap_or(text.len());
        return run_execution_unit_probe(
            ExecutionUnitProbeRequest {
                text: &text,
                profile: requested_profile,
                cursor_offset,
                expected_unit: expected_unit.as_deref(),
                expected_range: expected_unit_range,
            },
            output_json,
            summary,
        );
    }
    if let Some(editor_op) = editor_op {
        let cursor_offset = cursor_offset.unwrap_or(text.len());
        return run_editor_op_probe(
            EditorOpProbeRequest {
                text: &text,
                profile: requested_profile,
                op: editor_op,
                cursor_offset,
                selection_start,
                selection_end,
                line_comment_prefix_override: None,
                expected_edits: &expected_edits,
            },
            output_json,
            summary,
        );
    }
    let viewport = viewport_range_for_text(&text, viewport_start, viewport_bytes)?;
    if full_rope && viewport.is_some() {
        return Err(anyhow::anyhow!(
            "--full-rope cannot be combined with --viewport-start or --viewport-bytes"
        ));
    }

    let (highlights, timing) = if full_rope {
        measure_highlight_rope(&mut highlighter, &text, repeat)
    } else if let Some(viewport) = viewport.clone() {
        measure_highlight_rope_range(&mut highlighter, &text, viewport, repeat, show_phase_timing)
    } else {
        measure_highlight(&mut highlighter, &text, repeat, show_phase_timing)
    };
    let coverage = if let Some(viewport) = viewport.clone() {
        highlight_coverage_for_range(&text, viewport, &highlights)
    } else {
        highlight_coverage(&text, &highlights)
    };
    let assignment_check = ad_hoc_assignment_check(
        &text,
        &highlights,
        &expected_assignments,
        &rejected_assignments,
    );
    let render_profile =
        render_runs.then(|| measure_render_highlight_runs(&text, &highlights, repeat));
    let rendered_highlights = render_profile.as_ref().map(|profile| &profile.highlights);
    let render_invariant_check = rendered_highlights.map(|rendered_highlights| {
        render_invariant_check(
            &text,
            rendered_highlights,
            RenderInvariantMode::FullDocument,
        )
    });
    let render_assignment_check = rendered_highlights.map(|rendered_highlights| {
        render_ad_hoc_assignment_check(
            &text,
            rendered_highlights,
            &expected_render_assignments,
            &rejected_render_assignments,
        )
    });
    let token_report = (!token_reports.is_empty()).then(|| {
        token_reports
            .iter()
            .map(|token| {
                token_assignment_report(
                    &text,
                    &highlights,
                    rendered_highlights.map_or(&[] as &[Highlight], |highlights| highlights),
                    token,
                )
            })
            .collect::<Vec<_>>()
    });
    if output_json {
        let assignments = highlights
            .iter()
            .map(|highlight| {
                serde_json::json!({
                    "start": highlight.start,
                    "end": highlight.end,
                    "kind": highlight_kind_name(highlight.kind),
                    "text": &text[highlight.start..highlight.end],
                })
            })
            .collect::<Vec<_>>();
        let rendered_assignments = rendered_highlights.map(|rendered_highlights| {
            rendered_highlights
                .iter()
                .map(|highlight| {
                    serde_json::json!({
                        "start": highlight.start,
                        "end": highlight.end,
                        "kind": highlight_kind_name(highlight.kind),
                        "text": &text[highlight.start..highlight.end],
                    })
                })
                .collect::<Vec<_>>()
        });
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "profile": highlighter.language_profile(),
                "driver": driver,
                "bytes": text.len(),
                "full_rope": full_rope,
                "viewport": viewport,
                "coverage": coverage,
                "timing": timing,
                "assignment_check": assignment_check,
                "render_assignment_check": render_assignment_check,
                "render_invariant_check": render_invariant_check,
                "token_report": token_report,
                "render_timing": render_profile.as_ref().map(|profile| &profile.timing),
                "highlights": assignments,
                "rendered_highlights": rendered_assignments,
            }))?
        );
        check_fail_under(&coverage, fail_under)?;
        if let Some(render_profile) = &render_profile {
            check_timing_under(&render_profile.timing, fail_render_avg_ms)?;
        }
        check_ad_hoc_assignments(&assignment_check)?;
        if let Some(render_assignment_check) = &render_assignment_check {
            check_ad_hoc_assignments(render_assignment_check)?;
        }
        if let Some(render_invariant_check) = &render_invariant_check {
            check_render_invariants(render_invariant_check)?;
        }
        return Ok(());
    }

    if summary {
        let suffix = format!(
            "{}{}",
            if full_rope { " full_rope=true" } else { "" },
            viewport
                .as_ref()
                .map(|range| format!(" viewport={}..{}", range.start, range.end))
                .unwrap_or_default()
        );
        println!(
            "profile={} bytes={}{} highlights={}",
            highlighter.language_profile(),
            text.len(),
            suffix,
            highlights.len()
        );
    } else {
        let suffix = format!(
            "{}{}",
            if full_rope { "\nfull_rope=true" } else { "" },
            viewport
                .as_ref()
                .map(|range| format!("\nviewport={}..{}", range.start, range.end))
                .unwrap_or_default()
        );
        println!(
            "profile={}\nbytes={}{}\nhighlights={}",
            highlighter.language_profile(),
            text.len(),
            suffix,
            highlights.len()
        );
        for highlight in &highlights {
            println!(
                "{:>6}..{:<6} {:<11} {}",
                highlight.start,
                highlight.end,
                format!("{:?}", highlight.kind),
                debug_token(&text[highlight.start..highlight.end])
            );
        }
    }
    if show_coverage {
        print_coverage(&text, &coverage);
    }
    if show_timing {
        print_timing(&timing);
        if let Some(render_profile) = &render_profile {
            print_render_timing(&render_profile.timing, render_profile.highlights.len());
        }
    }
    print_ad_hoc_assignment_check(&assignment_check);
    if let Some(token_report) = &token_report {
        print_token_assignment_reports(token_report);
    }
    if let Some(rendered_highlights) = rendered_highlights {
        if !summary {
            println!("rendered_highlights={}", rendered_highlights.len());
            for highlight in rendered_highlights {
                println!(
                    "{:>6}..{:<6} Render      {:<11} {}",
                    highlight.start,
                    highlight.end,
                    format!("{:?}", highlight.kind),
                    debug_token(&text[highlight.start..highlight.end])
                );
            }
        }
        if let Some(render_assignment_check) = &render_assignment_check {
            print_ad_hoc_assignment_check(render_assignment_check);
        }
        if let Some(render_invariant_check) = &render_invariant_check {
            print_render_invariant_check(render_invariant_check);
        }
    }

    check_fail_under(&coverage, fail_under)?;
    check_timing_under(&timing, fail_avg_ms)?;
    check_phase_timing_under(&timing, fail_phase_avg_ms)?;
    if let Some(render_profile) = &render_profile {
        check_timing_under(&render_profile.timing, fail_render_avg_ms)?;
    }
    check_ad_hoc_assignments(&assignment_check)?;
    if let Some(render_assignment_check) = &render_assignment_check {
        check_ad_hoc_assignments(render_assignment_check)?;
    }
    if let Some(render_invariant_check) = &render_invariant_check {
        check_render_invariants(render_invariant_check)?;
    }

    Ok(())
}

fn print_coverage(text: &str, coverage: &HighlightCoverage) {
    println!(
        "coverage={:.2}% ({}/{})",
        coverage.percent, coverage.styled_non_ws_bytes, coverage.non_ws_bytes
    );
    if coverage.gaps.is_empty() {
        return;
    }

    println!("gaps={}", coverage.gaps.len());
    for gap in &coverage.gaps {
        println!("{:>6}..{:<6} Gap         {}", gap.start, gap.end, gap.text);
    }

    let trailing_gap_bytes = coverage
        .gaps
        .iter()
        .filter(|gap| {
            text[gap.start..gap.end]
                .chars()
                .all(|character| !character.is_whitespace())
        })
        .count();
    if trailing_gap_bytes > 0 {
        println!("gap_tokens={trailing_gap_bytes}");
    }
}

fn print_timing(timing: &HighlightTiming) {
    println!(
        "timing repeat={} total_ms={:.3} avg_ms={:.3}",
        timing.repeat, timing.total_ms, timing.avg_ms
    );
    if !timing.phases.is_empty() {
        for phase in &timing.phases {
            println!(
                "phase={} total_ms={:.3} avg_ms={:.3}",
                phase.name, phase.total_ms, phase.avg_ms
            );
        }
    }
}

fn print_render_timing(timing: &HighlightTiming, runs: usize) {
    println!(
        "render_timing repeat={} total_ms={:.3} avg_ms={:.3} runs={}",
        timing.repeat, timing.total_ms, timing.avg_ms, runs
    );
}

#[derive(Debug, Serialize)]
struct CapabilitiesProbeResult {
    profile: &'static str,
    driver: Option<String>,
    source: &'static str,
    tree_sitter_grammar: String,
    highlight_query_language: String,
    brackets: String,
    line_comment_prefix: Option<&'static str>,
    block_comment_delimiters: Option<(String, String)>,
    parameter_placeholders_enabled: bool,
    parameter_placeholders_question_mark: bool,
    command_syntax: bool,
    document_syntax: bool,
    sql_overlays: bool,
    dollar_quoted_strings: bool,
    formatter: String,
    indent_after_keywords: Vec<String>,
    auto_close_pairs: Vec<String>,
    folding_rules: String,
    execution_unit: String,
    document_symbols: String,
    overlays: String,
    markdown_fence_language: String,
    completion_triggers: Vec<String>,
    completion_word_chars: Vec<String>,
    check: CapabilityCheck,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedCapability {
    key: String,
    value: String,
}

#[derive(Debug, Serialize)]
struct CapabilityCheck {
    passed: bool,
    expected: Vec<ExpectedCapabilityCheck>,
}

#[derive(Debug, Serialize)]
struct ExpectedCapabilityCheck {
    key: String,
    expected: String,
    actual: Option<String>,
    matched: bool,
}

fn run_capabilities_probe(
    profile: &'static str,
    driver: Option<&str>,
    output_json: bool,
    summary: bool,
    expected_capabilities: &[ExpectedCapability],
) -> anyhow::Result<()> {
    let result = capabilities_probe_result(profile, driver, expected_capabilities)?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        check_capabilities(&result.check)?;
        return Ok(());
    }

    if summary {
        println!(
            "profile={} driver={} source={} grammar={} query={} brackets={} comment_prefix={} block_comment={} params={} params_question_mark={} command_syntax={} document_syntax={} sql_overlays={} dollar_quoted_strings={} formatter={} indent_after_keywords={} auto_close_pairs={} folding_rules={} execution_unit={} document_symbols={} overlays={} markdown_fence_language={} completion_triggers={} completion_word_chars={} check={}",
            result.profile,
            result.driver.as_deref().unwrap_or("<none>"),
            result.source,
            result.tree_sitter_grammar,
            result.highlight_query_language,
            result.brackets,
            result.line_comment_prefix.unwrap_or("<none>"),
            block_comment_delimiters_label(result.block_comment_delimiters.as_ref()),
            result.parameter_placeholders_enabled,
            result.parameter_placeholders_question_mark,
            result.command_syntax,
            result.document_syntax,
            result.sql_overlays,
            result.dollar_quoted_strings,
            result.formatter,
            result.indent_after_keywords.join(","),
            result.auto_close_pairs.join(","),
            result.folding_rules,
            result.execution_unit,
            result.document_symbols,
            result.overlays,
            result.markdown_fence_language,
            result.completion_triggers.join(""),
            result.completion_word_chars.join(""),
            result.check.passed
        );
        check_capabilities(&result.check)?;
        return Ok(());
    }

    println!("profile={}", result.profile);
    println!("driver={}", result.driver.as_deref().unwrap_or("<none>"));
    println!("source={}", result.source);
    println!("tree_sitter_grammar={}", result.tree_sitter_grammar);
    println!(
        "highlight_query_language={}",
        result.highlight_query_language
    );
    println!("brackets={}", result.brackets);
    println!(
        "line_comment_prefix={}",
        result.line_comment_prefix.unwrap_or("<none>")
    );
    println!(
        "block_comment_delimiters={}",
        block_comment_delimiters_label(result.block_comment_delimiters.as_ref())
    );
    println!(
        "parameter_placeholders_enabled={}",
        result.parameter_placeholders_enabled
    );
    println!(
        "parameter_placeholders_question_mark={}",
        result.parameter_placeholders_question_mark
    );
    println!("command_syntax={}", result.command_syntax);
    println!("document_syntax={}", result.document_syntax);
    println!("sql_overlays={}", result.sql_overlays);
    println!("dollar_quoted_strings={}", result.dollar_quoted_strings);
    println!("formatter={}", result.formatter);
    println!(
        "indent_after_keywords={}",
        result.indent_after_keywords.join(",")
    );
    println!("auto_close_pairs={}", result.auto_close_pairs.join(","));
    println!("folding_rules={}", result.folding_rules);
    println!("execution_unit={}", result.execution_unit);
    println!("document_symbols={}", result.document_symbols);
    println!("overlays={}", result.overlays);
    println!("markdown_fence_language={}", result.markdown_fence_language);
    println!(
        "completion_triggers={}",
        result.completion_triggers.join("")
    );
    println!(
        "completion_word_chars={}",
        result.completion_word_chars.join("")
    );
    print_capability_check(&result.check);
    check_capabilities(&result.check)?;

    Ok(())
}

fn capabilities_probe_result(
    profile: &'static str,
    driver: Option<&str>,
    expected_capabilities: &[ExpectedCapability],
) -> anyhow::Result<CapabilitiesProbeResult> {
    let (capabilities, completion_triggers, completion_word_chars, source) =
        capabilities_for_probe(profile, driver)?;
    let mut result = CapabilitiesProbeResult {
        profile: capabilities.profile,
        driver: driver.map(str::to_string),
        source,
        tree_sitter_grammar: format!("{:?}", capabilities.tree_sitter_grammar),
        highlight_query_language: format!("{:?}", capabilities.highlight_query_language),
        brackets: format!("{:?}", capabilities.brackets),
        line_comment_prefix: capabilities.line_comment_prefix,
        block_comment_delimiters: capabilities
            .block_comment_delimiters
            .map(|(start, end)| (start.to_string(), end.to_string())),
        parameter_placeholders_enabled: capabilities.parameter_placeholders.enabled,
        parameter_placeholders_question_mark: capabilities.parameter_placeholders.question_mark,
        command_syntax: capabilities.command_syntax,
        document_syntax: capabilities.document_syntax,
        sql_overlays: capabilities.sql_overlays,
        dollar_quoted_strings: capabilities.dollar_quoted_strings,
        formatter: format!("{:?}", capabilities.formatter),
        indent_after_keywords: capabilities
            .indent_after_keywords
            .iter()
            .map(|keyword| keyword.to_string())
            .collect(),
        auto_close_pairs: capabilities
            .auto_close_pairs
            .iter()
            .map(|(opener, closer)| format!("{opener}{closer}"))
            .collect(),
        folding_rules: folding_rules_label(capabilities.folding),
        execution_unit: format!("{:?}", capabilities.execution_unit),
        document_symbols: format!("{:?}", capabilities.document_symbols),
        overlays: format!("{:?}", capabilities.overlays),
        markdown_fence_language: markdown_fence_language_for_capabilities(&capabilities)
            .to_string(),
        completion_triggers,
        completion_word_chars,
        check: CapabilityCheck {
            passed: true,
            expected: Vec::new(),
        },
    };
    result.check = capability_check(&result, expected_capabilities);
    Ok(result)
}

fn capabilities_for_probe(
    profile: &'static str,
    driver: Option<&str>,
) -> anyhow::Result<(
    zqlz_core::SyntaxDriverCapabilities,
    Vec<String>,
    Vec<String>,
    &'static str,
)> {
    if let Some(driver) = driver {
        #[cfg(not(feature = "syntax-probe-drivers"))]
        {
            let _ = driver;
            return Err(anyhow::anyhow!(
                "`--driver --capabilities` requires `syntax-probe-drivers` or `syntax-probe-all-drivers` feature"
            ));
        }

        #[cfg(feature = "syntax-probe-drivers")]
        {
            let Some(metadata) = get_syntax_metadata_for_driver(driver) else {
                return Err(anyhow::anyhow!(
                    "unknown driver or missing syntax metadata: {driver}"
                ));
            };
            return Ok((
                metadata.capabilities,
                metadata
                    .completion_triggers
                    .into_iter()
                    .map(|character| character.to_string())
                    .collect(),
                metadata
                    .completion_word_chars
                    .into_iter()
                    .map(|character| character.to_string())
                    .collect(),
                "driver-bundle",
            ));
        }
    }

    Ok((
        get_syntax_driver_capabilities(profile),
        Vec::new(),
        Vec::new(),
        "core-profile",
    ))
}

fn block_comment_delimiters_label(delimiters: Option<&(String, String)>) -> String {
    delimiters
        .map(|(start, end)| format!("{start}{end}"))
        .unwrap_or_else(|| "<none>".to_string())
}

fn folding_rules_label(rules: zqlz_core::SyntaxFoldingRules) -> String {
    format!(
        "begin_end={},case={},function={},parenthesis={}",
        rules.begin_end_blocks,
        rules.case_blocks,
        rules.function_definitions,
        rules.parenthesis_blocks
    )
}

fn parse_expected_capability(raw: &str) -> anyhow::Result<ExpectedCapability> {
    let (key, value) = raw
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("--expect-capability needs KEY=VALUE"))?;
    Ok(ExpectedCapability {
        key: key.to_string(),
        value: value.to_string(),
    })
}

fn capability_check(
    result: &CapabilitiesProbeResult,
    expected_capabilities: &[ExpectedCapability],
) -> CapabilityCheck {
    let expected = expected_capabilities
        .iter()
        .map(|expected| {
            let actual = capability_value(result, &expected.key);
            let matched = actual.as_deref() == Some(expected.value.as_str());
            ExpectedCapabilityCheck {
                key: expected.key.clone(),
                expected: expected.value.clone(),
                actual,
                matched,
            }
        })
        .collect::<Vec<_>>();
    let passed = expected.iter().all(|check| check.matched);
    CapabilityCheck { passed, expected }
}

fn capability_value(result: &CapabilitiesProbeResult, key: &str) -> Option<String> {
    Some(match key {
        "profile" => result.profile.to_string(),
        "driver" => result.driver.as_deref().unwrap_or("<none>").to_string(),
        "source" => result.source.to_string(),
        "tree_sitter_grammar" => result.tree_sitter_grammar.clone(),
        "highlight_query_language" => result.highlight_query_language.clone(),
        "brackets" => result.brackets.clone(),
        "line_comment_prefix" => result.line_comment_prefix.unwrap_or("<none>").to_string(),
        "block_comment_delimiters" => {
            block_comment_delimiters_label(result.block_comment_delimiters.as_ref())
        }
        "parameter_placeholders_enabled" => result.parameter_placeholders_enabled.to_string(),
        "parameter_placeholders_question_mark" => {
            result.parameter_placeholders_question_mark.to_string()
        }
        "command_syntax" => result.command_syntax.to_string(),
        "document_syntax" => result.document_syntax.to_string(),
        "sql_overlays" => result.sql_overlays.to_string(),
        "dollar_quoted_strings" => result.dollar_quoted_strings.to_string(),
        "formatter" => result.formatter.clone(),
        "indent_after_keywords" => result.indent_after_keywords.join(","),
        "auto_close_pairs" => result.auto_close_pairs.join(","),
        "folding_rules" => result.folding_rules.clone(),
        "execution_unit" => result.execution_unit.clone(),
        "document_symbols" => result.document_symbols.clone(),
        "overlays" => result.overlays.clone(),
        "markdown_fence_language" => result.markdown_fence_language.clone(),
        "completion_triggers" => result.completion_triggers.join(""),
        "completion_word_chars" => result.completion_word_chars.join(""),
        _ => return None,
    })
}

fn print_capability_check(check: &CapabilityCheck) {
    for check in &check.expected {
        println!(
            "capability key={} expected={} actual={} matched={}",
            check.key,
            check.expected,
            check.actual.as_deref().unwrap_or("<unknown>"),
            check.matched
        );
    }
}

fn check_capabilities(check: &CapabilityCheck) -> anyhow::Result<()> {
    for check in &check.expected {
        if !check.matched {
            return Err(anyhow::anyhow!(
                "expected capability {}={} but got {}",
                check.key,
                check.expected,
                check.actual.as_deref().unwrap_or("<unknown>")
            ));
        }
    }

    Ok(())
}

#[derive(Clone)]
struct ExecutionUnitProbeRequest<'a> {
    text: &'a str,
    profile: &'static str,
    cursor_offset: usize,
    expected_unit: Option<&'a str>,
    expected_range: Option<std::ops::Range<usize>>,
}

#[derive(Debug, Serialize)]
struct ExecutionUnitProbeResult {
    profile: &'static str,
    bytes: usize,
    cursor: usize,
    range_start: usize,
    range_end: usize,
    sql_span: bool,
    unit: String,
    check: ExecutionUnitCheck,
}

#[derive(Debug, Serialize)]
struct ExecutionUnitCheck {
    passed: bool,
    unit_matched: Option<bool>,
    range_matched: Option<bool>,
}

fn run_execution_unit_probe(
    request: ExecutionUnitProbeRequest<'_>,
    output_json: bool,
    summary: bool,
) -> anyhow::Result<()> {
    let result = execution_unit_probe_result(request);

    if output_json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        check_execution_unit(&result.check)?;
        return Ok(());
    }

    if summary {
        println!(
            "profile={} bytes={} cursor={} unit_range={}..{} sql_span={} unit={} check={}",
            result.profile,
            result.bytes,
            result.cursor,
            result.range_start,
            result.range_end,
            result.sql_span,
            debug_token(&result.unit),
            result.check.passed
        );
        check_execution_unit(&result.check)?;
        return Ok(());
    }

    println!("profile={}", result.profile);
    println!("bytes={}", result.bytes);
    println!("cursor={}", result.cursor);
    println!("unit_range={}..{}", result.range_start, result.range_end);
    println!("sql_span={}", result.sql_span);
    println!("unit={}", debug_token(&result.unit));
    println!("check={}", result.check.passed);
    check_execution_unit(&result.check)?;

    Ok(())
}

fn execution_unit_probe_result(request: ExecutionUnitProbeRequest<'_>) -> ExecutionUnitProbeResult {
    let cursor_offset =
        clamp_to_char_boundary_in_str(request.text, request.cursor_offset.min(request.text.len()));
    let unit = execution_unit_for_capabilities(
        request.text,
        cursor_offset,
        &get_syntax_driver_capabilities(request.profile),
    );
    let unit_matched = request
        .expected_unit
        .map(|expected| unit.source == expected);
    let range_matched = request
        .expected_range
        .as_ref()
        .map(|expected| unit.byte_range == *expected);
    let passed = unit_matched.unwrap_or(true) && range_matched.unwrap_or(true);

    ExecutionUnitProbeResult {
        profile: request.profile,
        bytes: request.text.len(),
        cursor: cursor_offset,
        range_start: unit.byte_range.start,
        range_end: unit.byte_range.end,
        sql_span: unit.sql_span.is_some(),
        unit: unit.source,
        check: ExecutionUnitCheck {
            passed,
            unit_matched,
            range_matched,
        },
    }
}

fn check_execution_unit(check: &ExecutionUnitCheck) -> anyhow::Result<()> {
    if !check.passed {
        return Err(anyhow::anyhow!(
            "execution unit expectation failed: unit_matched={:?} range_matched={:?}",
            check.unit_matched,
            check.range_matched
        ));
    }

    Ok(())
}

fn parse_byte_range_arg(raw: &str) -> anyhow::Result<std::ops::Range<usize>> {
    let (start, end) = raw
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("range needs START:END"))?;
    Ok(start.parse::<usize>()?..end.parse::<usize>()?)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EditorProbeOp {
    ToggleComment,
    ToggleBlockComment,
    Newline,
    AutoClose,
}

fn parse_editor_op(op: &str) -> anyhow::Result<EditorProbeOp> {
    match op {
        "toggle-comment" => Ok(EditorProbeOp::ToggleComment),
        "toggle-block-comment" => Ok(EditorProbeOp::ToggleBlockComment),
        "newline" => Ok(EditorProbeOp::Newline),
        "auto-close" => Ok(EditorProbeOp::AutoClose),
        _ => Err(anyhow::anyhow!(
            "unknown editor op: {op}; expected toggle-comment|toggle-block-comment|newline|auto-close"
        )),
    }
}

#[derive(Debug, Serialize)]
struct EditorOpProbeResult {
    profile: &'static str,
    bytes: usize,
    cursor: usize,
    selection_start: usize,
    selection_end: usize,
    line_comment_prefix: Option<&'static str>,
    op: &'static str,
    edit_check: EditorEditCheck,
    edits: Vec<EditorProbeEdit>,
}

#[derive(Debug, Serialize)]
struct EditorProbeEdit {
    start: usize,
    end: usize,
    replacement: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ExpectedEditorEdit {
    start: usize,
    end: usize,
    replacement: String,
}

#[derive(Debug, Serialize)]
struct EditorEditCheck {
    passed: bool,
    expected: Vec<ExpectedEditorEditCheck>,
}

#[derive(Debug, Serialize)]
struct ExpectedEditorEditCheck {
    start: usize,
    end: usize,
    replacement: String,
    matched: bool,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverEditorContractCheck {
    driver: String,
    profile: &'static str,
    passed: bool,
    bracket_mode: String,
    bracket_pairs: usize,
    bracket_expected: bool,
    bracket_passed: bool,
    line_comment_prefix: Option<&'static str>,
    comment_toggle_edits: usize,
    comment_toggle_passed: bool,
    block_comment_edits: usize,
    block_comment_passed: bool,
    folding_comment_regions: usize,
    folding_comment_passed: bool,
    folding_structural_regions: usize,
    folding_structural_passed: bool,
    newline_edits: usize,
    newline_passed: bool,
    auto_close_edits: usize,
    auto_close_passed: bool,
}

#[derive(Clone, Copy)]
struct EditorOpProbeRequest<'a> {
    text: &'a str,
    profile: &'static str,
    op: EditorProbeOp,
    cursor_offset: usize,
    selection_start: Option<usize>,
    selection_end: Option<usize>,
    line_comment_prefix_override: Option<Option<&'static str>>,
    expected_edits: &'a [ExpectedEditorEdit],
}

fn run_editor_op_probe(
    request: EditorOpProbeRequest<'_>,
    output_json: bool,
    summary: bool,
) -> anyhow::Result<()> {
    let result = editor_op_probe_result(request)?;

    if output_json {
        println!("{}", serde_json::to_string_pretty(&result)?);
        check_editor_edits(&result.edit_check)?;
        return Ok(());
    }

    if summary {
        println!(
            "profile={} bytes={} cursor={} selection={}..{} op={} comment_prefix={} edits={} edit_check={}",
            result.profile,
            result.bytes,
            result.cursor,
            result.selection_start,
            result.selection_end,
            result.op,
            result.line_comment_prefix.unwrap_or("<none>"),
            result.edits.len(),
            result.edit_check.passed
        );
        check_editor_edits(&result.edit_check)?;
        return Ok(());
    }

    println!("profile={}", result.profile);
    println!("bytes={}", result.bytes);
    println!("cursor={}", result.cursor);
    println!(
        "selection={}..{}",
        result.selection_start, result.selection_end
    );
    println!("op={}", result.op);
    println!(
        "comment_prefix={}",
        result.line_comment_prefix.unwrap_or("<none>")
    );
    println!("edits={}", result.edits.len());
    for edit in &result.edits {
        println!(
            "{:>6}..{:<6} Edit        {}",
            edit.start,
            edit.end,
            debug_token(&edit.replacement)
        );
    }
    print_editor_edit_check(&result.edit_check);
    check_editor_edits(&result.edit_check)?;

    Ok(())
}

fn editor_op_probe_result(
    request: EditorOpProbeRequest<'_>,
) -> anyhow::Result<EditorOpProbeResult> {
    let text = request.text;
    let buffer = TextBuffer::new(text);
    let cursor_offset = clamp_to_char_boundary_in_str(text, request.cursor_offset.min(text.len()));
    let selection_start = clamp_to_char_boundary_in_str(
        text,
        request
            .selection_start
            .unwrap_or(cursor_offset)
            .min(text.len()),
    );
    let selection_end = clamp_to_char_boundary_in_str(
        text,
        request
            .selection_end
            .unwrap_or(cursor_offset)
            .min(text.len()),
    );
    let cursor = Cursor::at(buffer.offset_to_position(cursor_offset)?);
    let selection = Selection::from_anchor_head(
        buffer.offset_to_position(selection_start)?,
        buffer.offset_to_position(selection_end)?,
    );
    let snapshot = EditorCoreSnapshot::new(
        &buffer,
        SelectionsCollection::single(cursor, selection),
        false,
        Vec::new(),
    );
    let capabilities = get_syntax_driver_capabilities(request.profile);
    let line_comment_prefix = request
        .line_comment_prefix_override
        .unwrap_or(capabilities.line_comment_prefix);
    let (op_name, edits) = match request.op {
        EditorProbeOp::ToggleComment => {
            let edits = snapshot
                .line_prefix_edit_batch(
                    4,
                    false,
                    LinePrefixEditMode::ToggleComment,
                    line_comment_prefix,
                )
                .map(|batch| editor_probe_edits(batch.edits))
                .unwrap_or_default();
            ("toggle-comment", edits)
        }
        EditorProbeOp::ToggleBlockComment => {
            let edits = capabilities
                .block_comment_delimiters
                .and_then(|(start_delimiter, end_delimiter)| {
                    snapshot.toggle_block_comment_plan(start_delimiter, end_delimiter)
                })
                .map(|plan| editor_probe_edits(plan.batch.edits))
                .unwrap_or_default();
            ("toggle-block-comment", edits)
        }
        EditorProbeOp::Newline => {
            let edits = snapshot
                .auto_indent_newline_text(
                    true,
                    "    ",
                    line_comment_prefix,
                    &capabilities.indent_after_keywords,
                )
                .map(|plan| {
                    vec![EditorProbeEdit {
                        start: cursor_offset,
                        end: cursor_offset,
                        replacement: plan.text,
                    }]
                })
                .unwrap_or_default();
            ("newline", edits)
        }
        EditorProbeOp::AutoClose => {
            let edits = capabilities
                .auto_close_pairs
                .first()
                .and_then(|(opener, closer)| {
                    snapshot.auto_close_bracket_plan(*opener, *closer, true, false)
                })
                .map(|plan| editor_probe_edits(plan.batch.edits))
                .unwrap_or_default();
            ("auto-close", edits)
        }
    };
    let result = EditorOpProbeResult {
        profile: request.profile,
        bytes: text.len(),
        cursor: cursor_offset,
        selection_start,
        selection_end,
        line_comment_prefix,
        op: op_name,
        edit_check: editor_edit_check(&edits, request.expected_edits),
        edits,
    };
    Ok(result)
}

fn parse_expected_edit(raw: &str) -> anyhow::Result<ExpectedEditorEdit> {
    let (start, rest) = raw
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("--expect-edit needs START:END:TEXT"))?;
    let (end, replacement) = rest
        .split_once(':')
        .ok_or_else(|| anyhow::anyhow!("--expect-edit needs START:END:TEXT"))?;

    Ok(ExpectedEditorEdit {
        start: start.parse::<usize>()?,
        end: end.parse::<usize>()?,
        replacement: unescape_expected_edit_text(replacement),
    })
}

fn unescape_expected_edit_text(text: &str) -> String {
    text.replace("\\n", "\n").replace("\\t", "\t")
}

fn editor_edit_check(
    edits: &[EditorProbeEdit],
    expected_edits: &[ExpectedEditorEdit],
) -> EditorEditCheck {
    let expected = expected_edits
        .iter()
        .map(|expected| {
            let matched = edits.iter().any(|edit| {
                edit.start == expected.start
                    && edit.end == expected.end
                    && edit.replacement == expected.replacement
            });
            ExpectedEditorEditCheck {
                start: expected.start,
                end: expected.end,
                replacement: expected.replacement.clone(),
                matched,
            }
        })
        .collect::<Vec<_>>();
    let passed = expected.iter().all(|check| check.matched);
    EditorEditCheck { passed, expected }
}

fn print_editor_edit_check(edit_check: &EditorEditCheck) {
    for check in &edit_check.expected {
        println!(
            "edit_expect range={}..{} replacement={} matched={}",
            check.start,
            check.end,
            debug_token(&check.replacement),
            check.matched
        );
    }
}

fn check_editor_edits(edit_check: &EditorEditCheck) -> anyhow::Result<()> {
    for check in &edit_check.expected {
        if !check.matched {
            return Err(anyhow::anyhow!(
                "expected edit {}..{} replacement {:?} was not found",
                check.start,
                check.end,
                check.replacement
            ));
        }
    }

    Ok(())
}

fn editor_probe_edits(edits: Vec<TextReplacementEdit>) -> Vec<EditorProbeEdit> {
    edits
        .into_iter()
        .map(|edit| EditorProbeEdit {
            start: edit.range.start,
            end: edit.range.end,
            replacement: edit.replacement,
        })
        .collect()
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_editor_contract_check(
    driver: &str,
    profile: &'static str,
    fixture: &ProbeSyntaxQualityFixture,
) -> anyhow::Result<DriverEditorContractCheck> {
    let metadata = get_syntax_metadata_for_driver(driver)
        .ok_or_else(|| anyhow::anyhow!("missing syntax metadata for driver: {driver}"))?;
    let capabilities = metadata.capabilities;
    let bracket_mode = syntax_bracket_scan_mode_for_capabilities(&capabilities);
    let bracket_pairs = first_driver_fixture_bracket_pair_count(&capabilities, &fixture.text);
    let bracket_expected = fixture
        .text
        .bytes()
        .any(|byte| matches!(byte, b'(' | b')' | b'[' | b']' | b'{' | b'}'));
    let bracket_passed = !bracket_expected || bracket_pairs > 0;

    let line_comment_prefix = capabilities.line_comment_prefix;
    let comment_text = driver_editor_contract_comment_text(profile);
    let comment_toggle = editor_op_probe_result(EditorOpProbeRequest {
        text: comment_text,
        profile,
        op: EditorProbeOp::ToggleComment,
        cursor_offset: 0,
        selection_start: Some(0),
        selection_end: Some(comment_text.len()),
        line_comment_prefix_override: Some(line_comment_prefix),
        expected_edits: &[],
    })?;
    let comment_toggle_passed = if line_comment_prefix.is_some() {
        !comment_toggle.edits.is_empty()
    } else {
        comment_toggle.edits.is_empty()
    };

    let block_comment = editor_op_probe_result(EditorOpProbeRequest {
        text: comment_text,
        profile,
        op: EditorProbeOp::ToggleBlockComment,
        cursor_offset: 0,
        selection_start: Some(0),
        selection_end: Some(comment_text.len()),
        line_comment_prefix_override: Some(line_comment_prefix),
        expected_edits: &[],
    })?;
    let block_comment_passed = if capabilities.block_comment_delimiters.is_some() {
        block_comment.edits.len() == 2
    } else {
        block_comment.edits.is_empty()
    };

    let folding_text = driver_editor_contract_multiline_comment_text();
    let folding_regions = detect_folds_with_block_comments(
        &TextBuffer::new(folding_text),
        capabilities.block_comment_delimiters,
    );
    let folding_comment_regions = folding_regions
        .iter()
        .filter(|region| region.kind == FoldKind::Comment)
        .count();
    let folding_comment_passed = if capabilities.block_comment_delimiters.is_some() {
        folding_comment_regions > 0
    } else {
        folding_comment_regions == 0
    };
    let structural_folding_text = "BEGIN\nSELECT (\n1\n);\nEND;";
    let structural_folding_regions = detect_folds_with_rules(
        &TextBuffer::new(structural_folding_text),
        capabilities.block_comment_delimiters,
        capabilities.folding,
    );
    let folding_structural_regions = structural_folding_regions.len();
    let expects_structural_folds =
        capabilities.folding.begin_end_blocks || capabilities.folding.parenthesis_blocks;
    let folding_structural_passed = if expects_structural_folds {
        folding_structural_regions > 0
    } else {
        folding_structural_regions == 0
    };

    let newline_text = driver_editor_contract_newline_text(profile);
    let newline = editor_op_probe_result(EditorOpProbeRequest {
        text: newline_text,
        profile,
        op: EditorProbeOp::Newline,
        cursor_offset: newline_text.len(),
        selection_start: None,
        selection_end: None,
        line_comment_prefix_override: Some(line_comment_prefix),
        expected_edits: &[],
    })?;
    let newline_passed = newline
        .edits
        .first()
        .is_some_and(|edit| edit.replacement.starts_with('\n'));

    let auto_close = editor_op_probe_result(EditorOpProbeRequest {
        text: "",
        profile,
        op: EditorProbeOp::AutoClose,
        cursor_offset: 0,
        selection_start: None,
        selection_end: None,
        line_comment_prefix_override: Some(line_comment_prefix),
        expected_edits: &[],
    })?;
    let expected_auto_close = capabilities
        .auto_close_pairs
        .first()
        .map(|(opener, closer)| format!("{opener}{closer}"));
    let auto_close_passed = match expected_auto_close {
        Some(expected) => auto_close
            .edits
            .first()
            .is_some_and(|edit| edit.replacement == expected),
        None => auto_close.edits.is_empty(),
    };

    let passed = bracket_passed
        && comment_toggle_passed
        && block_comment_passed
        && folding_comment_passed
        && folding_structural_passed
        && newline_passed
        && auto_close_passed;
    Ok(DriverEditorContractCheck {
        driver: driver.to_string(),
        profile,
        passed,
        bracket_mode: format!("{bracket_mode:?}"),
        bracket_pairs,
        bracket_expected,
        bracket_passed,
        line_comment_prefix,
        comment_toggle_edits: comment_toggle.edits.len(),
        comment_toggle_passed,
        block_comment_edits: block_comment.edits.len(),
        block_comment_passed,
        folding_comment_regions,
        folding_comment_passed,
        folding_structural_regions,
        folding_structural_passed,
        newline_edits: newline.edits.len(),
        newline_passed,
        auto_close_edits: auto_close.edits.len(),
        auto_close_passed,
    })
}

#[cfg(feature = "syntax-probe-drivers")]
fn first_driver_fixture_bracket_pair_count(
    capabilities: &zqlz_core::SyntaxDriverCapabilities,
    text: &str,
) -> usize {
    for cursor in text
        .bytes()
        .enumerate()
        .filter_map(|(index, byte)| matches!(byte, b')' | b']' | b'}').then_some(index))
    {
        let pairs = syntax_bracket_pairs_for_capabilities(capabilities, text, 0, cursor);
        if !pairs.is_empty() {
            return pairs.len();
        }
    }

    0
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_editor_contract_comment_text(profile: &str) -> &'static str {
    match profile {
        "mongodb" => "db.orders.find({ status: \"paid\" })",
        "redis" => "GET user:1",
        _ => "SELECT 1;",
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_editor_contract_newline_text(profile: &str) -> &'static str {
    match profile {
        "mongodb" => "db.orders.find({",
        "redis" => "HSET user:1",
        _ => "SELECT (\n    1",
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_editor_contract_multiline_comment_text() -> &'static str {
    "SELECT 1;\n/* driver block comment\n   folds only when driver enables it */\nSELECT 2;"
}

#[cfg(feature = "syntax-probe-drivers")]
fn check_driver_editor_contract(check: &DriverEditorContractCheck) -> anyhow::Result<()> {
    if check.passed {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "{} editor contract failed: bracket_passed={} comment_toggle_passed={} block_comment_passed={} folding_comment_passed={} folding_structural_passed={} newline_passed={} auto_close_passed={}",
        check.driver,
        check.bracket_passed,
        check.comment_toggle_passed,
        check.block_comment_passed,
        check.folding_comment_passed,
        check.folding_structural_passed,
        check.newline_passed,
        check.auto_close_passed
    ))
}

fn run_bracket_probe(
    text: &str,
    profile: &'static str,
    cursor_offset: usize,
    output_json: bool,
    summary: bool,
) -> anyhow::Result<()> {
    let cursor_offset = clamp_to_char_boundary_in_str(text, cursor_offset.min(text.len()));
    let mode = syntax_bracket_scan_mode_for_profile(profile);
    let pairs = syntax_bracket_pairs_for_profile(profile, text, 0, cursor_offset);

    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "profile": profile,
                "bytes": text.len(),
                "cursor": cursor_offset,
                "mode": format!("{mode:?}"),
                "pairs": pairs.iter().map(|pair| {
                    serde_json::json!({
                        "open": pair.open,
                        "close": pair.close,
                        "open_text": &text[pair.open..pair.open + 1],
                        "close_text": &text[pair.close..pair.close + 1],
                    })
                }).collect::<Vec<_>>(),
            }))?
        );
        return Ok(());
    }

    if summary {
        println!(
            "profile={profile} bytes={} cursor={cursor_offset} bracket_mode={mode:?} pairs={}",
            text.len(),
            pairs.len()
        );
        return Ok(());
    }

    println!("profile={profile}");
    println!("bytes={}", text.len());
    println!("cursor={cursor_offset}");
    println!("bracket_mode={mode:?}");
    println!("pairs={}", pairs.len());
    for pair in pairs {
        println!(
            "{:>6}..{:<6} BracketPair {}{}",
            pair.open,
            pair.close,
            debug_token(&text[pair.open..pair.open + 1]),
            debug_token(&text[pair.close..pair.close + 1])
        );
    }

    Ok(())
}

fn run_outline_probe(
    text: &str,
    profile: &'static str,
    output_json: bool,
    summary: bool,
    expected_symbols: &[String],
) -> anyhow::Result<()> {
    let symbols = driver_document_symbols(profile, text);
    let symbol_check = outline_symbol_check(&symbols, expected_symbols);
    if output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "profile": profile,
                "bytes": text.len(),
                "symbol_check": symbol_check,
                "symbols": symbols.iter().map(|symbol| {
                    serde_json::json!({
                        "label": symbol.label,
                        "line": symbol.line,
                        "column": symbol.column,
                        "source_range": {"start": symbol.source_range.start, "end": symbol.source_range.end},
                        "target_range": symbol.target_range.as_ref().map(|range| {
                            serde_json::json!({"start": range.start, "end": range.end})
                        }),
                        "source": &text[symbol.source_range.clone()],
                        "target": symbol.target_range.as_ref().map(|range| &text[range.clone()]),
                    })
                }).collect::<Vec<_>>(),
            }))?
        );
        check_outline_symbols(&symbol_check)?;
        return Ok(());
    }

    println!(
        "profile={profile} bytes={} symbols={}",
        text.len(),
        symbols.len()
    );
    if summary {
        for symbol in &symbols {
            println!("{}:{} {}", symbol.line + 1, symbol.column + 1, symbol.label);
        }
        print_outline_symbol_check(&symbol_check);
        check_outline_symbols(&symbol_check)?;
        return Ok(());
    }

    for symbol in &symbols {
        println!(
            "{:>4}:{:<4} {:<32} source={}..{}{}",
            symbol.line + 1,
            symbol.column + 1,
            symbol.label,
            symbol.source_range.start,
            symbol.source_range.end,
            outline_target_suffix(symbol),
        );
    }
    print_outline_symbol_check(&symbol_check);
    check_outline_symbols(&symbol_check)?;
    Ok(())
}

fn outline_target_suffix(symbol: &SqlDocumentSymbol) -> String {
    symbol
        .target_range
        .as_ref()
        .map(|range| format!(" target={}..{}", range.start, range.end))
        .unwrap_or_default()
}

#[derive(Debug, Serialize)]
struct OutlineSymbolCheck {
    passed: bool,
    expected: Vec<ExpectedOutlineSymbolCheck>,
}

#[derive(Debug, Serialize)]
struct ExpectedOutlineSymbolCheck {
    label: String,
    matched: bool,
    line: Option<usize>,
    column: Option<usize>,
}

fn outline_symbol_check(
    symbols: &[SqlDocumentSymbol],
    expected_symbols: &[String],
) -> OutlineSymbolCheck {
    let expected = expected_symbols
        .iter()
        .map(|expected_label| {
            let symbol = symbols
                .iter()
                .find(|symbol| symbol.label == *expected_label);
            ExpectedOutlineSymbolCheck {
                label: expected_label.clone(),
                matched: symbol.is_some(),
                line: symbol.map(|symbol| symbol.line),
                column: symbol.map(|symbol| symbol.column),
            }
        })
        .collect::<Vec<_>>();
    let passed = expected.iter().all(|check| check.matched);

    OutlineSymbolCheck { passed, expected }
}

fn print_outline_symbol_check(symbol_check: &OutlineSymbolCheck) {
    for check in &symbol_check.expected {
        println!(
            "outline label={} matched={}{}",
            debug_token(&check.label),
            check.matched,
            check
                .line
                .zip(check.column)
                .map(|(line, column)| format!(" at={}:{}", line + 1, column + 1))
                .unwrap_or_default()
        );
    }
}

fn check_outline_symbols(symbol_check: &OutlineSymbolCheck) -> anyhow::Result<()> {
    for check in &symbol_check.expected {
        if !check.matched {
            return Err(anyhow::anyhow!(
                "expected outline symbol {:?} was not found",
                check.label
            ));
        }
    }

    Ok(())
}

fn generated_large_sql_fixture(statements: usize) -> String {
    let mut source = String::with_capacity(statements.saturating_mul(96));
    source.push_str(
        "CREATE TABLE events (id UUID, payload JSONB, created_at TIMESTAMPTZ, amount DECIMAL(12, 2));\n",
    );
    for index in 0..statements {
        source.push_str(&format!(
            "SELECT jsonb_extract_path_text(payload, 'name') AS name_{index}, amount::numeric FROM events WHERE id = ${} AND created_at >= NOW() - INTERVAL '1 day';\n",
            (index % 16) + 1
        ));
    }
    source
}

fn viewport_range_for_text(
    text: &str,
    viewport_start: Option<usize>,
    viewport_bytes: Option<usize>,
) -> anyhow::Result<Option<std::ops::Range<usize>>> {
    match (viewport_start, viewport_bytes) {
        (None, None) => Ok(None),
        (Some(_), None) => Err(anyhow::anyhow!(
            "--viewport-start requires --viewport-bytes"
        )),
        (None, Some(_)) => Err(anyhow::anyhow!(
            "--viewport-bytes requires --viewport-start"
        )),
        (Some(start), Some(bytes)) => {
            if start > text.len() {
                return Err(anyhow::anyhow!(
                    "--viewport-start {start} is beyond input length {}",
                    text.len()
                ));
            }
            let mut end = start.saturating_add(bytes).min(text.len());
            while end < text.len() && !text.is_char_boundary(end) {
                end += 1;
            }
            let mut start = start;
            while start > 0 && !text.is_char_boundary(start) {
                start -= 1;
            }
            if start >= end {
                return Err(anyhow::anyhow!("viewport range is empty"));
            }
            Ok(Some(start..end))
        }
    }
}

fn clamp_to_char_boundary_in_str(text: &str, mut offset: usize) -> usize {
    offset = offset.min(text.len());
    while offset > 0 && !text.is_char_boundary(offset) {
        offset -= 1;
    }
    offset
}

fn highlight_coverage_for_range(
    text: &str,
    range: std::ops::Range<usize>,
    highlights: &[Highlight],
) -> HighlightCoverage {
    let range_text = &text[range.clone()];
    let shifted_highlights = highlights
        .iter()
        .filter_map(|highlight| {
            let start = highlight.start.max(range.start);
            let end = highlight.end.min(range.end);
            (start < end).then_some(Highlight {
                start: start - range.start,
                end: end - range.start,
                kind: highlight.kind,
            })
        })
        .collect::<Vec<_>>();
    highlight_coverage(range_text, &shifted_highlights)
}

#[derive(Clone, Copy)]
struct FixtureProbeOptions {
    output_json: bool,
    fail_under: Option<f64>,
    repeat: usize,
    show_timing: bool,
    fail_avg_ms: Option<f64>,
    fail_phase_avg_ms: Option<f64>,
    fail_render_avg_ms: Option<f64>,
    full_rope: bool,
}

fn run_fixture_probe(fixture_name: &str, options: FixtureProbeOptions) -> anyhow::Result<()> {
    let fixtures = if fixture_name == "all" {
        syntax_quality_fixtures().to_vec()
    } else {
        let fixture = syntax_quality_fixtures()
            .iter()
            .copied()
            .find(|fixture| fixture.profile == normalize_syntax_profile(fixture_name))
            .ok_or_else(|| anyhow::anyhow!("unknown fixture: {fixture_name}"))?;
        vec![fixture]
    };

    let mut results = Vec::with_capacity(fixtures.len());
    for fixture in fixtures {
        let mut highlighter = SyntaxHighlighter::new().map_err(anyhow::Error::msg)?;
        highlighter.set_language_profile(fixture.profile);
        let (highlights, timing) = if options.full_rope {
            measure_highlight_rope(&mut highlighter, fixture.text, options.repeat)
        } else {
            measure_highlight(
                &mut highlighter,
                fixture.text,
                options.repeat,
                options.fail_phase_avg_ms.is_some(),
            )
        };
        let coverage = highlight_coverage(fixture.text, &highlights);
        check_fail_under(&coverage, options.fail_under)?;
        check_timing_under(&timing, options.fail_avg_ms)?;
        check_phase_timing_under(&timing, options.fail_phase_avg_ms)?;
        let assignment_check = fixture_assignment_check(&fixture, &highlights);
        check_fixture_assignments(&fixture, &assignment_check)?;
        let render_profile =
            measure_render_highlight_runs(fixture.text, &highlights, options.repeat);
        check_timing_under(&render_profile.timing, options.fail_render_avg_ms)?;
        let render_assignment_check =
            render_fixture_assignment_check(&fixture, &render_profile.highlights);
        check_fixture_assignments(&fixture, &render_assignment_check)?;
        let render_invariant_check = render_invariant_check(
            fixture.text,
            &render_profile.highlights,
            RenderInvariantMode::FullDocument,
        );
        check_render_invariants(&render_invariant_check)?;
        let display_chunks = display_chunks_for_highlights(fixture.text, &highlights);
        let display_chunk_check = display_chunk_check(&display_chunks);
        check_display_chunks(&display_chunk_check)?;
        let display_assignment_check = display_fixture_assignment_check(&fixture, &display_chunks);
        check_fixture_assignments(&fixture, &display_assignment_check)?;

        if options.output_json {
            results.push(serde_json::json!({
                "profile": highlighter.language_profile(),
                "bytes": fixture.text.len(),
                "full_rope": options.full_rope,
                "coverage": coverage,
                "assignment_check": assignment_check,
                "render_assignment_check": render_assignment_check,
                "render_invariant_check": render_invariant_check,
                "display_chunk_check": display_chunk_check,
                "display_assignment_check": display_assignment_check,
                "render_timing": render_profile.timing,
                "timing": timing,
                "highlights": highlights.iter().map(|highlight| {
                    serde_json::json!({
                        "start": highlight.start,
                        "end": highlight.end,
                        "kind": highlight_kind_name(highlight.kind),
                        "text": &fixture.text[highlight.start..highlight.end],
                    })
                }).collect::<Vec<_>>(),
            }));
        } else {
            println!(
                "fixture={} bytes={} full_rope={} highlights={} coverage={:.2}% ({}/{})",
                highlighter.language_profile(),
                fixture.text.len(),
                options.full_rope,
                highlights.len(),
                coverage.percent,
                coverage.styled_non_ws_bytes,
                coverage.non_ws_bytes
            );
            if !coverage.gaps.is_empty() {
                println!("gaps={}", coverage.gaps.len());
                for gap in &coverage.gaps {
                    println!("{:>6}..{:<6} Gap         {}", gap.start, gap.end, gap.text);
                }
            }
            if options.show_timing {
                print_timing(&timing);
                print_render_timing(&render_profile.timing, render_profile.highlights.len());
            }
            println!(
                "render_assignments={} expected={} rejected={} passed={}",
                highlighter.language_profile(),
                render_assignment_check.expected.len(),
                render_assignment_check.rejected.len(),
                render_assignment_check.passed
            );
            print_render_invariant_check(&render_invariant_check);
            print_display_chunk_check(&display_chunk_check);
            println!(
                "display_assignments={} expected={} rejected={} passed={}",
                highlighter.language_profile(),
                display_assignment_check.expected.len(),
                display_assignment_check.rejected.len(),
                display_assignment_check.passed
            );
        }
    }

    if options.output_json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({ "fixtures": results }))?
        );
    }

    Ok(())
}

fn run_driver_fixture_probe(
    driver_fixture: &str,
    options: FixtureProbeOptions,
) -> anyhow::Result<()> {
    #[cfg(not(feature = "syntax-probe-drivers"))]
    {
        let _ = (driver_fixture, options);
        return Err(anyhow::anyhow!(
            "`--driver-fixture` requires `syntax-probe-drivers` or `syntax-probe-all-drivers` feature"
        ));
    }

    #[cfg(feature = "syntax-probe-drivers")]
    {
        let drivers = if driver_fixture == "all" {
            let mut seen = HashSet::new();
            let mut drivers = DriverRegistry::with_defaults()
                .list()
                .into_iter()
                .map(str::to_string)
                .filter(|driver_name| seen.insert(driver_name.clone()))
                .collect::<Vec<_>>();
            drivers.sort_unstable();
            drivers
        } else {
            vec![driver_fixture.to_string()]
        };

        let mut results = Vec::with_capacity(drivers.len());
        for driver in drivers {
            let profile = profile_for_driver(&driver)?;
            let fixture = driver_syntax_quality_fixture(&driver, profile)?;

            let mut highlighter = SyntaxHighlighter::new().map_err(anyhow::Error::msg)?;
            highlighter.set_language_profile(profile);
            apply_driver_syntax_metadata(&mut highlighter, &driver)?;
            let (highlights, timing) = if options.full_rope {
                measure_highlight_rope(&mut highlighter, &fixture.text, options.repeat)
            } else {
                measure_highlight(
                    &mut highlighter,
                    &fixture.text,
                    options.repeat,
                    options.fail_phase_avg_ms.is_some(),
                )
            };
            let coverage = highlight_coverage(&fixture.text, &highlights);
            check_fail_under(&coverage, options.fail_under)?;
            check_timing_under(&timing, options.fail_avg_ms)?;
            check_phase_timing_under(&timing, options.fail_phase_avg_ms)?;
            let assignment_check = probe_fixture_assignment_check(&fixture, &highlights);
            check_probe_fixture_assignments(&fixture, &assignment_check)?;
            let render_profile =
                measure_render_highlight_runs(&fixture.text, &highlights, options.repeat);
            check_timing_under(&render_profile.timing, options.fail_render_avg_ms)?;
            let render_assignment_check =
                render_probe_fixture_assignment_check(&fixture, &render_profile.highlights);
            check_probe_fixture_assignments(&fixture, &render_assignment_check)?;
            let render_invariant_check = render_invariant_check(
                &fixture.text,
                &render_profile.highlights,
                RenderInvariantMode::FullDocument,
            );
            check_render_invariants(&render_invariant_check)?;
            let display_chunks = display_chunks_for_highlights(&fixture.text, &highlights);
            let display_chunk_check = display_chunk_check(&display_chunks);
            check_display_chunks(&display_chunk_check)?;
            let display_assignment_check =
                display_probe_fixture_assignment_check(&fixture, &display_chunks);
            check_probe_fixture_assignments(&fixture, &display_assignment_check)?;
            let viewport_assignment_check =
                viewport_probe_fixture_assignment_check(&mut highlighter, &driver, &fixture)?;
            check_viewport_assignments(&viewport_assignment_check)?;
            let metadata = get_syntax_metadata_for_driver(&driver)
                .ok_or_else(|| anyhow::anyhow!("missing syntax metadata for driver: {driver}"))?;
            let outline_symbols =
                driver_document_symbols_for_capabilities(&metadata.capabilities, &fixture.text);
            let outline_check = outline_symbol_check(&outline_symbols, &fixture.expected_outline);
            check_outline_symbols(&outline_check)?;
            let editor_contract = driver_editor_contract_check(&driver, profile, &fixture)?;
            check_driver_editor_contract(&editor_contract)?;
            let driver_term_check = driver_term_smoke_check(&driver)?;
            let completion_contract = driver_completion_contract_check(&driver)?;
            check_driver_completion_contract(&completion_contract)?;
            let capability_contract = driver_capability_contract_check(&driver, profile)?;
            check_capabilities(&capability_contract)?;

            if options.output_json {
                results.push(serde_json::json!({
                    "driver": driver,
                    "profile": highlighter.language_profile(),
                    "bytes": fixture.text.len(),
                    "full_rope": options.full_rope,
                    "fixture_source": fixture.source,
                    "coverage": coverage,
                    "assignment_check": assignment_check,
                    "render_assignment_check": render_assignment_check,
                    "render_invariant_check": render_invariant_check,
                    "display_chunk_check": display_chunk_check,
                    "display_assignment_check": display_assignment_check,
                    "viewport_assignment_check": viewport_assignment_check,
                    "outline_check": outline_check,
                    "editor_contract": editor_contract,
                    "render_timing": render_profile.timing,
                    "driver_term_check": driver_term_check,
                    "completion_contract": completion_contract,
                    "capability_contract": capability_contract,
                    "timing": timing,
                    "highlights": highlights.iter().map(|highlight| {
                        serde_json::json!({
                            "start": highlight.start,
                            "end": highlight.end,
                            "kind": highlight_kind_name(highlight.kind),
                            "text": &fixture.text[highlight.start..highlight.end],
                        })
                    }).collect::<Vec<_>>(),
                }));
            } else {
                println!(
                    "driver={} profile={} bytes={} full_rope={} highlights={} coverage={:.2}% ({}/{})",
                    driver,
                    highlighter.language_profile(),
                    fixture.text.len(),
                    options.full_rope,
                    highlights.len(),
                    coverage.percent,
                    coverage.styled_non_ws_bytes,
                    coverage.non_ws_bytes
                );
                println!("fixture_source={}", fixture.source);
                if !coverage.gaps.is_empty() {
                    println!("gaps={}", coverage.gaps.len());
                    for gap in &coverage.gaps {
                        println!("{:>6}..{:<6} Gap         {}", gap.start, gap.end, gap.text);
                    }
                }
                if options.show_timing {
                    print_timing(&timing);
                    print_render_timing(&render_profile.timing, render_profile.highlights.len());
                }
                if let Some(driver_term_check) = driver_term_check {
                    println!(
                        "driver_terms={} checked={} render_checked={} passed={}",
                        driver_term_check.driver,
                        driver_term_check.expected.len(),
                        driver_term_check.render_expected.len(),
                        driver_term_check.passed
                    );
                }
                println!(
                    "render_assignments={} expected={} rejected={} passed={}",
                    driver,
                    render_assignment_check.expected.len(),
                    render_assignment_check.rejected.len(),
                    render_assignment_check.passed
                );
                print_render_invariant_check(&render_invariant_check);
                print_display_chunk_check(&display_chunk_check);
                println!(
                    "display_assignments={} expected={} rejected={} passed={}",
                    driver,
                    display_assignment_check.expected.len(),
                    display_assignment_check.rejected.len(),
                    display_assignment_check.passed
                );
                println!(
                    "viewport_assignments={} checked={} passed={}",
                    driver,
                    viewport_assignment_check.expected.len()
                        + viewport_assignment_check.rejected.len(),
                    viewport_assignment_check.passed
                );
                println!(
                    "outline={} symbols={} expected={} passed={}",
                    driver,
                    outline_symbols.len(),
                    outline_check.expected.len(),
                    outline_check.passed
                );
                println!(
                    "editor_contract={} bracket_pairs={} comment_edits={} block_comment_edits={} folding_comment_regions={} folding_structural_regions={} newline_edits={} auto_close_edits={} passed={}",
                    driver,
                    editor_contract.bracket_pairs,
                    editor_contract.comment_toggle_edits,
                    editor_contract.block_comment_edits,
                    editor_contract.folding_comment_regions,
                    editor_contract.folding_structural_regions,
                    editor_contract.newline_edits,
                    editor_contract.auto_close_edits,
                    editor_contract.passed
                );
                println!(
                    "completion={} expected_label={} prefix={} fallback_prefix={} suggestions={} fallback_suggestions={} trigger_checks={} word_char_checks={} metadata_checks={} passed={}",
                    driver,
                    completion_contract
                        .expected_label
                        .as_deref()
                        .unwrap_or("<none>"),
                    completion_contract.prefix.as_deref().unwrap_or("<none>"),
                    completion_contract
                        .fallback_prefix
                        .as_deref()
                        .unwrap_or("<none>"),
                    completion_contract.matched_labels.len(),
                    completion_contract.fallback_matched_labels.len(),
                    completion_contract.trigger_checks.len(),
                    completion_contract.word_char_checks.len(),
                    completion_contract.metadata_checks.len(),
                    completion_contract.passed
                );
                println!(
                    "capabilities={} checked={} passed={}",
                    driver,
                    capability_contract.expected.len(),
                    capability_contract.passed
                );
            }
        }

        if options.output_json {
            println!(
                "{}",
                serde_json::to_string_pretty(&serde_json::json!({ "drivers": results }))?
            );
        }

        Ok(())
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_capability_contract_check(
    driver: &str,
    profile: &'static str,
) -> anyhow::Result<CapabilityCheck> {
    let expected = expected_capabilities_from_driver_metadata(driver, profile)?;
    let result = capabilities_probe_result(profile, Some(driver), &expected)?;
    Ok(result.check)
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_completion_contract_check(driver: &str) -> anyhow::Result<DriverCompletionContractCheck> {
    let metadata = get_syntax_metadata_for_driver(driver)
        .ok_or_else(|| anyhow::anyhow!("unknown driver or missing syntax metadata: {driver}"))?;
    let terms = metadata.syntax_terms;
    let mut labels = terms.keywords;
    labels.extend(terms.functions);
    labels.extend(terms.types);
    labels.sort();
    labels.dedup();

    let expected_label = labels.first().cloned();
    let prefix = expected_label.as_deref().map(completion_probe_prefix);
    let matched_labels = prefix
        .as_deref()
        .map(|prefix| completion_probe_matches(&labels, prefix))
        .unwrap_or_default();
    let fallback_provider = SqlCompletionProvider::for_capabilities(&metadata.capabilities);
    let fallback_prefix = prefix.as_deref().map(|prefix| {
        let text = ropey::Rope::from_str(prefix);
        fallback_provider.completion_prefix(&text, prefix.len())
    });
    let fallback_prefix_passed = prefix
        .as_ref()
        .zip(fallback_prefix.as_ref())
        .is_some_and(|(expected, actual)| expected == actual);
    let fallback_matched_labels = fallback_prefix
        .as_deref()
        .map(|prefix| {
            fallback_provider
                .get_word_completions(prefix)
                .into_iter()
                .take(16)
                .map(|item| item.label)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    let label_passed = expected_label
        .as_ref()
        .is_some_and(|label| matched_labels.iter().any(|matched| matched == label));
    let fallback_label_passed = expected_label.as_ref().is_some_and(|label| {
        fallback_matched_labels
            .iter()
            .any(|matched| matched == label)
    });

    let trigger_checks = metadata
        .completion_triggers
        .into_iter()
        .map(|character| DriverCompletionTriggerCheck {
            character,
            expected_kind: "trigger",
            passed: fallback_provider
                .completion_trigger_context_for_text(&character.to_string())
                .is_some_and(|context| {
                    context.trigger_kind == lsp_types::CompletionTriggerKind::TRIGGER_CHARACTER
                        && context.trigger_character.as_deref() == Some(&character.to_string())
                }),
        })
        .collect::<Vec<_>>();
    let word_char_checks = metadata
        .completion_word_chars
        .into_iter()
        .filter(|character| {
            !trigger_checks
                .iter()
                .any(|check| check.character == *character)
        })
        .map(|character| DriverCompletionTriggerCheck {
            character,
            expected_kind: "invoked",
            passed: fallback_provider
                .completion_trigger_context_for_text(&character.to_string())
                .is_some_and(|context| {
                    context.trigger_kind == lsp_types::CompletionTriggerKind::INVOKED
                        && context.trigger_character.is_none()
                }),
        })
        .collect::<Vec<_>>();
    let metadata_checks = driver_completion_metadata_checks(&metadata.completions);
    let passed = label_passed
        && fallback_label_passed
        && fallback_prefix_passed
        && !trigger_checks.is_empty()
        && !word_char_checks.is_empty()
        && trigger_checks.iter().all(|check| check.passed)
        && word_char_checks.iter().all(|check| check.passed)
        && metadata_checks.iter().all(|check| check.passed);

    Ok(DriverCompletionContractCheck {
        driver: driver.to_string(),
        passed,
        expected_label,
        prefix,
        fallback_prefix,
        matched_labels,
        fallback_matched_labels,
        trigger_checks,
        word_char_checks,
        metadata_checks,
    })
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_completion_metadata_checks(
    completions: &zqlz_core::dialect_config::CompletionsConfig,
) -> Vec<DriverCompletionMetadataCheck> {
    let mut checks = Vec::new();

    if let Some(keyword) = completions
        .keywords
        .iter()
        .find(|keyword| keyword.snippet.is_some())
    {
        checks.push(DriverCompletionMetadataCheck {
            label: keyword.name.clone(),
            kind: "keyword_snippet",
            passed: keyword
                .snippet
                .as_deref()
                .is_some_and(|snippet| snippet.contains("${") && snippet.contains(&keyword.name)),
        });
    }

    if let Some(keyword) = completions
        .keywords
        .iter()
        .find(|keyword| keyword.documentation.is_some() || keyword.description.is_some())
    {
        checks.push(DriverCompletionMetadataCheck {
            label: keyword.name.clone(),
            kind: "keyword_documentation",
            passed: keyword
                .documentation
                .as_deref()
                .or(keyword.description.as_deref())
                .is_some_and(|documentation| !documentation.trim().is_empty()),
        });
    }

    if let Some(function) = completions.functions.iter().find(|function| {
        function.signature.is_some()
            || function.return_type.is_some()
            || function.documentation.is_some()
            || function.description.is_some()
    }) {
        checks.push(DriverCompletionMetadataCheck {
            label: function.name.clone(),
            kind: "function_detail",
            passed: function
                .signature
                .as_deref()
                .or(function.return_type.as_deref())
                .or(function.documentation.as_deref())
                .or(function.description.as_deref())
                .is_some_and(|detail| !detail.trim().is_empty()),
        });
    }

    if let Some(data_type) = completions
        .data_types
        .iter()
        .find(|data_type| data_type.description.is_some())
    {
        checks.push(DriverCompletionMetadataCheck {
            label: data_type.name.clone(),
            kind: "type_documentation",
            passed: data_type
                .description
                .as_deref()
                .is_some_and(|documentation| !documentation.trim().is_empty()),
        });
    }

    if let Some(snippet) = completions.snippets.first() {
        checks.push(DriverCompletionMetadataCheck {
            label: snippet.name.clone(),
            kind: "named_snippet",
            passed: !snippet.prefix.trim().is_empty()
                && !snippet.body.trim().is_empty()
                && snippet.body.contains("${"),
        });
    }

    checks
}

#[cfg(feature = "syntax-probe-drivers")]
fn completion_probe_prefix(label: &str) -> String {
    label
        .chars()
        .take(4)
        .collect::<String>()
        .to_ascii_lowercase()
}

#[cfg(feature = "syntax-probe-drivers")]
fn completion_probe_matches(labels: &[String], prefix: &str) -> Vec<String> {
    labels
        .iter()
        .filter(|label| label.to_ascii_lowercase().starts_with(prefix))
        .take(16)
        .cloned()
        .collect()
}

#[cfg(feature = "syntax-probe-drivers")]
fn check_driver_completion_contract(check: &DriverCompletionContractCheck) -> anyhow::Result<()> {
    if check.passed {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "{} completion contract failed: expected_label={:?} prefix={:?} fallback_prefix={:?} suggestions={} fallback_suggestions={} trigger_checks={} word_char_checks={} metadata_checks={}",
        check.driver,
        check.expected_label,
        check.prefix,
        check.fallback_prefix,
        check.matched_labels.len(),
        check.fallback_matched_labels.len(),
        check.trigger_checks.len(),
        check.word_char_checks.len(),
        check.metadata_checks.len()
    ))
}

#[cfg(feature = "syntax-probe-drivers")]
fn expected_capabilities_from_driver_metadata(
    driver: &str,
    profile: &'static str,
) -> anyhow::Result<Vec<ExpectedCapability>> {
    let metadata = get_syntax_metadata_for_driver(driver)
        .ok_or_else(|| anyhow::anyhow!("unknown driver or missing syntax metadata: {driver}"))?;
    let capabilities = metadata.capabilities;
    let pairs = [
        ("source", "driver-bundle".to_string()),
        ("profile", profile.to_string()),
        (
            "tree_sitter_grammar",
            format!("{:?}", capabilities.tree_sitter_grammar),
        ),
        (
            "highlight_query_language",
            format!("{:?}", capabilities.highlight_query_language),
        ),
        ("brackets", format!("{:?}", capabilities.brackets)),
        (
            "line_comment_prefix",
            capabilities
                .line_comment_prefix
                .unwrap_or("<none>")
                .to_string(),
        ),
        (
            "block_comment_delimiters",
            capabilities
                .block_comment_delimiters
                .map(|(start, end)| format!("{start}{end}"))
                .unwrap_or_else(|| "<none>".to_string()),
        ),
        (
            "parameter_placeholders_enabled",
            capabilities.parameter_placeholders.enabled.to_string(),
        ),
        (
            "parameter_placeholders_question_mark",
            capabilities
                .parameter_placeholders
                .question_mark
                .to_string(),
        ),
        ("command_syntax", capabilities.command_syntax.to_string()),
        ("document_syntax", capabilities.document_syntax.to_string()),
        ("sql_overlays", capabilities.sql_overlays.to_string()),
        (
            "dollar_quoted_strings",
            capabilities.dollar_quoted_strings.to_string(),
        ),
        ("formatter", format!("{:?}", capabilities.formatter)),
        (
            "indent_after_keywords",
            capabilities.indent_after_keywords.join(","),
        ),
        (
            "auto_close_pairs",
            capabilities
                .auto_close_pairs
                .iter()
                .map(|(opener, closer)| format!("{opener}{closer}"))
                .collect::<Vec<_>>()
                .join(","),
        ),
        ("folding_rules", folding_rules_label(capabilities.folding)),
        (
            "execution_unit",
            format!("{:?}", capabilities.execution_unit),
        ),
        (
            "document_symbols",
            format!("{:?}", capabilities.document_symbols),
        ),
        ("overlays", format!("{:?}", capabilities.overlays)),
        (
            "markdown_fence_language",
            markdown_fence_language_for_capabilities(&capabilities).to_string(),
        ),
        (
            "completion_triggers",
            metadata.completion_triggers.into_iter().collect::<String>(),
        ),
        (
            "completion_word_chars",
            metadata
                .completion_word_chars
                .into_iter()
                .collect::<String>(),
        ),
    ];

    Ok(pairs
        .into_iter()
        .map(|(key, value)| ExpectedCapability {
            key: key.to_string(),
            value,
        })
        .collect())
}

#[derive(Debug, Serialize)]
struct HighlightTiming {
    repeat: usize,
    total_ms: f64,
    avg_ms: f64,
    phases: Vec<HighlightPhaseTiming>,
}

#[derive(Debug, Serialize)]
struct HighlightPhaseTiming {
    name: &'static str,
    total_ms: f64,
    avg_ms: f64,
}

#[derive(Debug)]
struct RenderHighlightProfile {
    highlights: Vec<Highlight>,
    timing: HighlightTiming,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RenderInvariantMode {
    FullDocument,
}

#[derive(Debug, Serialize)]
struct RenderInvariantCheck {
    passed: bool,
    runs: usize,
    covers_full_text: bool,
    contiguous: bool,
    char_boundaries: bool,
    no_empty_runs: bool,
    no_adjacent_same_kind: bool,
    first_start: Option<usize>,
    last_end: Option<usize>,
    failure: Option<String>,
}

#[derive(Debug, Serialize)]
struct DisplayChunkCheck {
    passed: bool,
    chunks: Vec<DisplayChunkSummary>,
    chunk_count: usize,
    highlight_count: usize,
    no_empty_runs: bool,
    no_overlaps: bool,
    char_boundaries: bool,
    failure: Option<String>,
}

#[derive(Debug, Serialize)]
struct DisplayChunkSummary {
    buffer_line: usize,
    start_offset: usize,
    text_len: usize,
    highlights: Vec<DisplayChunkHighlightSummary>,
}

#[derive(Debug, Serialize)]
struct DisplayChunkHighlightSummary {
    start: usize,
    end: usize,
    kind: &'static str,
}

fn measure_highlight(
    highlighter: &mut SyntaxHighlighter,
    text: &str,
    repeat: usize,
    include_phases: bool,
) -> (Vec<Highlight>, HighlightTiming) {
    let start = Instant::now();
    let mut highlights = Vec::new();
    let mut phases = Vec::new();
    for _ in 0..repeat {
        if include_phases {
            let profile = highlighter.highlight_profiled(text);
            highlights = profile.highlights;
            add_phase_timings(&mut phases, profile.phases);
        } else {
            highlights = highlighter.highlight(text);
        }
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    let phases = finish_phase_timings(phases, repeat);

    (
        highlights,
        HighlightTiming {
            repeat,
            total_ms,
            avg_ms: total_ms / repeat as f64,
            phases,
        },
    )
}

fn measure_highlight_rope(
    highlighter: &mut SyntaxHighlighter,
    text: &str,
    repeat: usize,
) -> (Vec<Highlight>, HighlightTiming) {
    let rope = ropey::Rope::from_str(text);
    let start = Instant::now();
    let mut highlights = Vec::new();
    for _ in 0..repeat {
        highlights = highlighter.highlight_rope(&rope);
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;

    (
        highlights,
        HighlightTiming {
            repeat,
            total_ms,
            avg_ms: total_ms / repeat as f64,
            phases: Vec::new(),
        },
    )
}

fn measure_render_highlight_runs(
    text: &str,
    highlights: &[Highlight],
    repeat: usize,
) -> RenderHighlightProfile {
    let start = Instant::now();
    let mut rendered_highlights = Vec::new();
    for _ in 0..repeat {
        rendered_highlights = render_highlight_runs(text, highlights);
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;

    RenderHighlightProfile {
        highlights: rendered_highlights,
        timing: HighlightTiming {
            repeat,
            total_ms,
            avg_ms: total_ms / repeat as f64,
            phases: Vec::new(),
        },
    }
}

fn render_invariant_check(
    text: &str,
    highlights: &[Highlight],
    mode: RenderInvariantMode,
) -> RenderInvariantCheck {
    let mut check = RenderInvariantCheck {
        passed: true,
        runs: highlights.len(),
        covers_full_text: true,
        contiguous: true,
        char_boundaries: true,
        no_empty_runs: true,
        no_adjacent_same_kind: true,
        first_start: highlights.first().map(|highlight| highlight.start),
        last_end: highlights.last().map(|highlight| highlight.end),
        failure: None,
    };

    match mode {
        RenderInvariantMode::FullDocument => {
            check.covers_full_text = match (highlights.first(), highlights.last()) {
                (Some(first), Some(last)) => first.start == 0 && last.end == text.len(),
                (None, None) => text.is_empty(),
                _ => false,
            };
            if !check.covers_full_text {
                check.failure = Some(format!(
                    "render runs do not cover full text: first_start={:?} last_end={:?} len={}",
                    check.first_start,
                    check.last_end,
                    text.len()
                ));
            }
        }
    }

    for (index, highlight) in highlights.iter().enumerate() {
        if highlight.start >= highlight.end {
            check.no_empty_runs = false;
            check
                .failure
                .get_or_insert_with(|| format!("empty render run at index {index}"));
        }
        if highlight.end > text.len()
            || !text.is_char_boundary(highlight.start)
            || !text.is_char_boundary(highlight.end)
        {
            check.char_boundaries = false;
            check.failure.get_or_insert_with(|| {
                format!(
                    "render run at index {index} has invalid UTF-8 boundaries {}..{}",
                    highlight.start, highlight.end
                )
            });
        }
        if let Some(previous) = index.checked_sub(1).and_then(|index| highlights.get(index)) {
            if previous.end != highlight.start {
                check.contiguous = false;
                check.failure.get_or_insert_with(|| {
                    format!(
                        "render run gap/overlap between {}..{} and {}..{}",
                        previous.start, previous.end, highlight.start, highlight.end
                    )
                });
            }
            if previous.kind == highlight.kind {
                check.no_adjacent_same_kind = false;
                check.failure.get_or_insert_with(|| {
                    format!(
                        "adjacent render runs share kind {} at {}..{} and {}..{}",
                        highlight_kind_name(highlight.kind),
                        previous.start,
                        previous.end,
                        highlight.start,
                        highlight.end
                    )
                });
            }
        }
    }

    check.passed = check.covers_full_text
        && check.contiguous
        && check.char_boundaries
        && check.no_empty_runs
        && check.no_adjacent_same_kind;
    check
}

fn measure_highlight_rope_range(
    highlighter: &mut SyntaxHighlighter,
    text: &str,
    byte_range: std::ops::Range<usize>,
    repeat: usize,
    include_phases: bool,
) -> (Vec<Highlight>, HighlightTiming) {
    let rope = ropey::Rope::from_str(text);
    let start = Instant::now();
    let mut highlights = Vec::new();
    let mut phases = Vec::new();
    for _ in 0..repeat {
        if include_phases {
            let profile = highlighter.highlight_rope_range_profiled(&rope, byte_range.clone());
            highlights = profile.highlights;
            add_phase_timings(&mut phases, profile.phases);
        } else {
            highlights = highlighter.highlight_rope_range(&rope, byte_range.clone());
        }
    }
    let total_ms = start.elapsed().as_secs_f64() * 1000.0;
    let phases = finish_phase_timings(phases, repeat);

    (
        highlights,
        HighlightTiming {
            repeat,
            total_ms,
            avg_ms: total_ms / repeat as f64,
            phases,
        },
    )
}

fn add_phase_timings(
    phase_totals: &mut Vec<(&'static str, f64)>,
    phases: Vec<SyntaxHighlightPhaseTiming>,
) {
    for phase in phases {
        if let Some((_, total_ms)) = phase_totals
            .iter_mut()
            .find(|(name, _)| *name == phase.name)
        {
            *total_ms += phase.elapsed_ms;
        } else {
            phase_totals.push((phase.name, phase.elapsed_ms));
        }
    }
}

fn finish_phase_timings(
    phase_totals: Vec<(&'static str, f64)>,
    repeat: usize,
) -> Vec<HighlightPhaseTiming> {
    phase_totals
        .into_iter()
        .map(|(name, total_ms)| HighlightPhaseTiming {
            name,
            total_ms,
            avg_ms: total_ms / repeat as f64,
        })
        .collect()
}

#[derive(Debug, Serialize)]
struct FixtureAssignmentCheck {
    passed: bool,
    expected: Vec<TokenAssignmentCheck>,
    rejected: Vec<TokenAssignmentCheck>,
}

#[derive(Debug, Serialize)]
struct TokenAssignmentCheck {
    token: String,
    kind: &'static str,
    assigned_kinds: Vec<&'static str>,
    passed: bool,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct ViewportAssignmentCheck {
    passed: bool,
    expected: Vec<ViewportTokenAssignmentCheck>,
    rejected: Vec<ViewportTokenAssignmentCheck>,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct ViewportTokenAssignmentCheck {
    token: String,
    kind: &'static str,
    range_start: usize,
    range_end: usize,
    raw_assigned_kinds: Vec<&'static str>,
    render_assigned_kinds: Vec<&'static str>,
    display_assigned_kinds: Vec<&'static str>,
    passed: bool,
}

#[derive(Debug, Clone)]
struct AssignmentExpectation {
    token: String,
    kind: HighlightKind,
}

#[derive(Debug, Serialize)]
struct AdHocAssignmentCheck {
    passed: bool,
    expected: Vec<TokenAssignmentCheck>,
    rejected: Vec<TokenAssignmentCheck>,
}

#[derive(Debug, Serialize)]
struct TokenAssignmentReport {
    token: String,
    occurrences: Vec<TokenOccurrenceReport>,
}

#[derive(Debug, Serialize)]
struct TokenOccurrenceReport {
    start: usize,
    end: usize,
    line: usize,
    column: usize,
    raw_assigned_kinds: Vec<&'static str>,
    render_assigned_kinds: Vec<&'static str>,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverTermSmokeCheck {
    driver: String,
    profile: &'static str,
    text: String,
    passed: bool,
    expected: Vec<DriverTermAssignmentCheck>,
    render_expected: Vec<DriverTermAssignmentCheck>,
    render_invariant_check: RenderInvariantCheck,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverCompletionContractCheck {
    driver: String,
    passed: bool,
    expected_label: Option<String>,
    prefix: Option<String>,
    fallback_prefix: Option<String>,
    matched_labels: Vec<String>,
    fallback_matched_labels: Vec<String>,
    trigger_checks: Vec<DriverCompletionTriggerCheck>,
    word_char_checks: Vec<DriverCompletionTriggerCheck>,
    metadata_checks: Vec<DriverCompletionMetadataCheck>,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverCompletionTriggerCheck {
    character: char,
    expected_kind: &'static str,
    passed: bool,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverCompletionMetadataCheck {
    label: String,
    kind: &'static str,
    passed: bool,
}

#[cfg(feature = "syntax-probe-drivers")]
struct ProbeSyntaxQualityFixture {
    profile: &'static str,
    source: &'static str,
    text: String,
    expected_tokens: Vec<(String, HighlightKind)>,
    rejected_tokens: Vec<(String, HighlightKind)>,
    expected_outline: Vec<String>,
}

#[cfg(feature = "syntax-probe-drivers")]
#[derive(Debug, Serialize)]
struct DriverTermAssignmentCheck {
    token: String,
    kind: &'static str,
    assigned_kinds: Vec<&'static str>,
    passed: bool,
}

fn fixture_assignment_check(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    highlights: &[Highlight],
) -> FixtureAssignmentCheck {
    fixture_assignment_check_with(fixture, highlights, assigned_kinds_for_token)
}

fn render_fixture_assignment_check(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    highlights: &[Highlight],
) -> FixtureAssignmentCheck {
    fixture_assignment_check_with(fixture, highlights, painted_kinds_for_token)
}

#[cfg(feature = "syntax-probe-drivers")]
fn probe_fixture_assignment_check(
    fixture: &ProbeSyntaxQualityFixture,
    highlights: &[Highlight],
) -> FixtureAssignmentCheck {
    probe_fixture_assignment_check_with(fixture, highlights, assigned_kinds_for_token)
}

#[cfg(feature = "syntax-probe-drivers")]
fn render_probe_fixture_assignment_check(
    fixture: &ProbeSyntaxQualityFixture,
    highlights: &[Highlight],
) -> FixtureAssignmentCheck {
    probe_fixture_assignment_check_with(fixture, highlights, painted_kinds_for_token)
}

fn display_fixture_assignment_check(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    chunks: &[DisplayTextChunk],
) -> FixtureAssignmentCheck {
    fixture_assignment_check_with_display_chunks(fixture, chunks, display_chunk_kinds_for_token)
}

#[cfg(feature = "syntax-probe-drivers")]
fn display_probe_fixture_assignment_check(
    fixture: &ProbeSyntaxQualityFixture,
    chunks: &[DisplayTextChunk],
) -> FixtureAssignmentCheck {
    probe_fixture_assignment_check_with_display_chunks(
        fixture,
        chunks,
        display_chunk_kinds_for_token,
    )
}

#[cfg(feature = "syntax-probe-drivers")]
fn viewport_probe_fixture_assignment_check(
    highlighter: &mut SyntaxHighlighter,
    driver: &str,
    fixture: &ProbeSyntaxQualityFixture,
) -> anyhow::Result<ViewportAssignmentCheck> {
    let expected = fixture
        .expected_tokens
        .iter()
        .map(|(token, kind)| {
            viewport_token_assignment_check(highlighter, driver, fixture, token, *kind, true)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let rejected = fixture
        .rejected_tokens
        .iter()
        .map(|(token, kind)| {
            viewport_token_assignment_check(highlighter, driver, fixture, token, *kind, false)
        })
        .collect::<anyhow::Result<Vec<_>>>()?;
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    Ok(ViewportAssignmentCheck {
        passed,
        expected,
        rejected,
    })
}

#[cfg(feature = "syntax-probe-drivers")]
fn viewport_token_assignment_check(
    highlighter: &mut SyntaxHighlighter,
    driver: &str,
    fixture: &ProbeSyntaxQualityFixture,
    token: &str,
    kind: HighlightKind,
    should_contain: bool,
) -> anyhow::Result<ViewportTokenAssignmentCheck> {
    let full_highlights = highlighter.highlight(&fixture.text);
    let full_render_highlights = render_highlight_runs(&fixture.text, &full_highlights);
    let occurrence = token_occurrence_for_viewport_check(
        &fixture.text,
        &full_render_highlights,
        token,
        kind,
        should_contain,
    )
    .ok_or_else(|| {
        anyhow::anyhow!(
            "{profile} {source} fixture token {token:?} has no suitable viewport occurrence",
            profile = fixture.profile,
            source = fixture.source
        )
    })?;
    let range = token_viewport_range(&fixture.text, occurrence, token.len());
    let rope = ropey::Rope::from_str(&fixture.text);
    let range_highlights = highlighter.highlight_rope_range(&rope, range.clone());
    let render_highlights = render_highlight_runs(&fixture.text, &range_highlights);
    let display_chunks = display_chunks_for_highlights(&fixture.text, &range_highlights);
    check_display_chunks(&display_chunk_check(&display_chunks))?;

    let raw_assigned = assigned_kinds_for_token(&fixture.text, &range_highlights, token);
    let render_assigned = painted_kinds_for_token(&fixture.text, &render_highlights, token);
    let display_assigned = display_chunk_kinds_for_token(&display_chunks, token);
    let raw_has_kind = raw_assigned.contains(&kind);
    let render_has_kind = render_assigned.contains(&kind);
    let display_has_kind = display_assigned.contains(&kind);
    let passed = if should_contain {
        raw_has_kind && render_has_kind && display_has_kind
    } else {
        !raw_has_kind && !render_has_kind && !display_has_kind
    };

    if !passed {
        let expectation = if should_contain {
            "expected"
        } else {
            "rejected"
        };
        return Err(anyhow::anyhow!(
            "{driver} viewport {expectation} {token:?} as {kind}; range={start}..{end} raw={raw:?} render={render:?} display={display:?}",
            kind = highlight_kind_name(kind),
            start = range.start,
            end = range.end,
            raw = raw_assigned
                .iter()
                .copied()
                .map(highlight_kind_name)
                .collect::<Vec<_>>(),
            render = render_assigned
                .iter()
                .copied()
                .map(highlight_kind_name)
                .collect::<Vec<_>>(),
            display = display_assigned
                .iter()
                .copied()
                .map(highlight_kind_name)
                .collect::<Vec<_>>(),
        ));
    }

    Ok(ViewportTokenAssignmentCheck {
        token: token.to_string(),
        kind: highlight_kind_name(kind),
        range_start: range.start,
        range_end: range.end,
        raw_assigned_kinds: raw_assigned.into_iter().map(highlight_kind_name).collect(),
        render_assigned_kinds: render_assigned
            .into_iter()
            .map(highlight_kind_name)
            .collect(),
        display_assigned_kinds: display_assigned
            .into_iter()
            .map(highlight_kind_name)
            .collect(),
        passed,
    })
}

#[cfg(feature = "syntax-probe-drivers")]
fn token_occurrence_for_viewport_check(
    text: &str,
    full_render_highlights: &[Highlight],
    token: &str,
    kind: HighlightKind,
    should_contain: bool,
) -> Option<usize> {
    let mut fallback = None;
    let mut search_start = 0usize;
    while let Some(relative_start) = text[search_start..].find(token) {
        let start = search_start + relative_start;
        let end = start + token.len();
        search_start = end;
        fallback.get_or_insert(start);

        let assigned = painted_kinds_for_range(full_render_highlights, start..end);
        if assigned.contains(&kind) == should_contain {
            return Some(start);
        }
    }
    fallback
}

fn painted_kinds_for_range(
    highlights: &[Highlight],
    range: std::ops::Range<usize>,
) -> Vec<HighlightKind> {
    let mut kinds = highlights
        .iter()
        .filter(|highlight| highlight.start < range.end && highlight.end > range.start)
        .map(|highlight| highlight.kind)
        .collect::<Vec<_>>();
    kinds.sort_by_key(|kind| highlight_kind_rank(*kind));
    kinds.dedup();
    kinds
}

#[cfg(feature = "syntax-probe-drivers")]
fn token_viewport_range(
    text: &str,
    token_start: usize,
    token_len: usize,
) -> std::ops::Range<usize> {
    let token_end = token_start.saturating_add(token_len).min(text.len());
    let line_start = text[..token_start]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line_end = text[token_end..]
        .find('\n')
        .map(|index| token_end + index + 1)
        .unwrap_or(text.len());
    let mut start = line_start.saturating_sub(96);
    let mut end = line_end.saturating_add(96).min(text.len());
    while start > 0 && !text.is_char_boundary(start) {
        start -= 1;
    }
    while end < text.len() && !text.is_char_boundary(end) {
        end += 1;
    }
    start..end
}

#[cfg(feature = "syntax-probe-drivers")]
fn check_viewport_assignments(check: &ViewportAssignmentCheck) -> anyhow::Result<()> {
    if check.passed {
        return Ok(());
    }

    Err(anyhow::anyhow!(
        "viewport assignment check failed: expected={:?} rejected={:?}",
        check
            .expected
            .iter()
            .filter(|check| !check.passed)
            .map(|check| (&check.token, check.kind))
            .collect::<Vec<_>>(),
        check
            .rejected
            .iter()
            .filter(|check| !check.passed)
            .map(|check| (&check.token, check.kind))
            .collect::<Vec<_>>()
    ))
}

fn fixture_assignment_check_with(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    highlights: &[Highlight],
    assigned_kinds: fn(&str, &[Highlight], &str) -> Vec<HighlightKind>,
) -> FixtureAssignmentCheck {
    let expected = fixture
        .expected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(fixture.text, highlights, token);
            let passed = assigned.contains(kind);
            TokenAssignmentCheck {
                token: (*token).to_string(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let rejected = fixture
        .rejected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(fixture.text, highlights, token);
            let passed = !assigned.contains(kind);
            TokenAssignmentCheck {
                token: (*token).to_string(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    FixtureAssignmentCheck {
        passed,
        expected,
        rejected,
    }
}

fn fixture_assignment_check_with_display_chunks(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    chunks: &[DisplayTextChunk],
    assigned_kinds: fn(&[DisplayTextChunk], &str) -> Vec<HighlightKind>,
) -> FixtureAssignmentCheck {
    let expected = fixture
        .expected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(chunks, token);
            let passed = assigned.contains(kind);
            TokenAssignmentCheck {
                token: (*token).to_string(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let rejected = fixture
        .rejected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(chunks, token);
            let passed = !assigned.contains(kind);
            TokenAssignmentCheck {
                token: (*token).to_string(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    FixtureAssignmentCheck {
        passed,
        expected,
        rejected,
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn probe_fixture_assignment_check_with(
    fixture: &ProbeSyntaxQualityFixture,
    highlights: &[Highlight],
    assigned_kinds: fn(&str, &[Highlight], &str) -> Vec<HighlightKind>,
) -> FixtureAssignmentCheck {
    let expected = fixture
        .expected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(&fixture.text, highlights, token);
            let passed = assigned.contains(kind);
            TokenAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let rejected = fixture
        .rejected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(&fixture.text, highlights, token);
            let passed = !assigned.contains(kind);
            TokenAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    FixtureAssignmentCheck {
        passed,
        expected,
        rejected,
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn probe_fixture_assignment_check_with_display_chunks(
    fixture: &ProbeSyntaxQualityFixture,
    chunks: &[DisplayTextChunk],
    assigned_kinds: fn(&[DisplayTextChunk], &str) -> Vec<HighlightKind>,
) -> FixtureAssignmentCheck {
    let expected = fixture
        .expected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(chunks, token);
            let passed = assigned.contains(kind);
            TokenAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let rejected = fixture
        .rejected_tokens
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds(chunks, token);
            let passed = !assigned.contains(kind);
            TokenAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    FixtureAssignmentCheck {
        passed,
        expected,
        rejected,
    }
}

fn check_fixture_assignments(
    fixture: &zqlz_text_editor::syntax::SyntaxQualityFixture,
    assignment_check: &FixtureAssignmentCheck,
) -> anyhow::Result<()> {
    let assignment_snapshot = assignment_check
        .expected
        .iter()
        .map(|check| (check.token.clone(), check.assigned_kinds.clone()))
        .collect::<Vec<_>>();

    for check in &assignment_check.expected {
        if !check.passed {
            let token = &check.token;
            let kind = check.kind;
            return Err(anyhow::anyhow!(
                "{profile} expected {token:?} as {kind}, assignment snapshot: {assignment_snapshot:?}",
                profile = fixture.profile
            ));
        }
    }

    for check in &assignment_check.rejected {
        if !check.passed {
            let token = &check.token;
            let kind = check.kind;
            return Err(anyhow::anyhow!(
                "{profile} did not expect {token:?} as {kind}, assignment snapshot: {assignment_snapshot:?}",
                profile = fixture.profile
            ));
        }
    }

    Ok(())
}

#[cfg(feature = "syntax-probe-drivers")]
fn check_probe_fixture_assignments(
    fixture: &ProbeSyntaxQualityFixture,
    assignment_check: &FixtureAssignmentCheck,
) -> anyhow::Result<()> {
    let assignment_snapshot = assignment_check
        .expected
        .iter()
        .map(|check| (check.token.clone(), check.assigned_kinds.clone()))
        .collect::<Vec<_>>();

    for check in &assignment_check.expected {
        if !check.passed {
            let token = &check.token;
            let kind = check.kind;
            return Err(anyhow::anyhow!(
                "{profile} {source} expected {token:?} as {kind}, assignment snapshot: {assignment_snapshot:?}",
                profile = fixture.profile,
                source = fixture.source
            ));
        }
    }

    for check in &assignment_check.rejected {
        if !check.passed {
            let token = &check.token;
            let kind = check.kind;
            return Err(anyhow::anyhow!(
                "{profile} {source} did not expect {token:?} as {kind}, assignment snapshot: {assignment_snapshot:?}",
                profile = fixture.profile,
                source = fixture.source
            ));
        }
    }

    Ok(())
}

fn parse_assignment_arg(input: &str) -> anyhow::Result<AssignmentExpectation> {
    let Some((token, kind)) = input.rsplit_once(':') else {
        return Err(anyhow::anyhow!(
            "assignment expectation must be TOKEN:KIND, got {input:?}"
        ));
    };
    if token.is_empty() {
        return Err(anyhow::anyhow!("assignment expectation token is empty"));
    }

    Ok(AssignmentExpectation {
        token: token.to_string(),
        kind: highlight_kind_from_name(kind)?,
    })
}

fn ad_hoc_assignment_check(
    text: &str,
    highlights: &[Highlight],
    expected_assignments: &[AssignmentExpectation],
    rejected_assignments: &[AssignmentExpectation],
) -> AdHocAssignmentCheck {
    ad_hoc_assignment_check_with(
        text,
        highlights,
        expected_assignments,
        rejected_assignments,
        assigned_kinds_for_token,
    )
}

fn render_ad_hoc_assignment_check(
    text: &str,
    highlights: &[Highlight],
    expected_assignments: &[AssignmentExpectation],
    rejected_assignments: &[AssignmentExpectation],
) -> AdHocAssignmentCheck {
    ad_hoc_assignment_check_with(
        text,
        highlights,
        expected_assignments,
        rejected_assignments,
        painted_kinds_for_token,
    )
}

fn ad_hoc_assignment_check_with(
    text: &str,
    highlights: &[Highlight],
    expected_assignments: &[AssignmentExpectation],
    rejected_assignments: &[AssignmentExpectation],
    assigned_kinds: fn(&str, &[Highlight], &str) -> Vec<HighlightKind>,
) -> AdHocAssignmentCheck {
    let expected = expected_assignments
        .iter()
        .map(|assignment| {
            let assigned = assigned_kinds(text, highlights, &assignment.token);
            let passed = assigned.contains(&assignment.kind);
            TokenAssignmentCheck {
                token: assignment.token.clone(),
                kind: highlight_kind_name(assignment.kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let rejected = rejected_assignments
        .iter()
        .map(|assignment| {
            let assigned = assigned_kinds(text, highlights, &assignment.token);
            let passed = !assigned.contains(&assignment.kind);
            TokenAssignmentCheck {
                token: assignment.token.clone(),
                kind: highlight_kind_name(assignment.kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed =
        expected.iter().all(|check| check.passed) && rejected.iter().all(|check| check.passed);

    AdHocAssignmentCheck {
        passed,
        expected,
        rejected,
    }
}

fn check_ad_hoc_assignments(assignment_check: &AdHocAssignmentCheck) -> anyhow::Result<()> {
    for check in &assignment_check.expected {
        if !check.passed {
            return Err(anyhow::anyhow!(
                "expected {token:?} as {kind}, assigned: {assigned:?}",
                token = check.token,
                kind = check.kind,
                assigned = check.assigned_kinds
            ));
        }
    }

    for check in &assignment_check.rejected {
        if !check.passed {
            return Err(anyhow::anyhow!(
                "rejected {token:?} as {kind}, assigned: {assigned:?}",
                token = check.token,
                kind = check.kind,
                assigned = check.assigned_kinds
            ));
        }
    }

    Ok(())
}

fn print_ad_hoc_assignment_check(assignment_check: &AdHocAssignmentCheck) {
    for check in assignment_check
        .expected
        .iter()
        .chain(assignment_check.rejected.iter())
    {
        println!(
            "assignment token={} kind={} assigned={:?} passed={}",
            debug_token(&check.token),
            check.kind,
            check.assigned_kinds,
            check.passed
        );
    }
}

fn token_assignment_report(
    text: &str,
    raw_highlights: &[Highlight],
    rendered_highlights: &[Highlight],
    token: &str,
) -> TokenAssignmentReport {
    let mut occurrences = Vec::new();
    let mut search_start = 0usize;
    while let Some(relative_start) = text[search_start..].find(token) {
        let start = search_start + relative_start;
        let end = start + token.len();
        search_start = end;
        let (line, column) = line_column_for_offset(text, start);
        occurrences.push(TokenOccurrenceReport {
            start,
            end,
            line,
            column,
            raw_assigned_kinds: painted_kinds_for_range(raw_highlights, start..end)
                .into_iter()
                .map(highlight_kind_name)
                .collect(),
            render_assigned_kinds: painted_kinds_for_range(rendered_highlights, start..end)
                .into_iter()
                .map(highlight_kind_name)
                .collect(),
        });
    }

    TokenAssignmentReport {
        token: token.to_string(),
        occurrences,
    }
}

fn line_column_for_offset(text: &str, offset: usize) -> (usize, usize) {
    let offset = offset.min(text.len());
    let line_start = text[..offset]
        .rfind('\n')
        .map(|index| index + 1)
        .unwrap_or(0);
    let line = text[..offset].bytes().filter(|byte| *byte == b'\n').count() + 1;
    let column = text[line_start..offset].chars().count() + 1;
    (line, column)
}

fn print_token_assignment_reports(reports: &[TokenAssignmentReport]) {
    for report in reports {
        if report.occurrences.is_empty() {
            println!(
                "token_report token={} occurrences=0",
                debug_token(&report.token)
            );
            continue;
        }
        for occurrence in &report.occurrences {
            println!(
                "token_report token={} range={}..{} line={} column={} raw={:?} render={:?}",
                debug_token(&report.token),
                occurrence.start,
                occurrence.end,
                occurrence.line,
                occurrence.column,
                occurrence.raw_assigned_kinds,
                occurrence.render_assigned_kinds
            );
        }
    }
}

fn print_render_invariant_check(check: &RenderInvariantCheck) {
    println!(
        "render_invariants runs={} covers_full_text={} contiguous={} char_boundaries={} no_empty_runs={} no_adjacent_same_kind={} passed={}",
        check.runs,
        check.covers_full_text,
        check.contiguous,
        check.char_boundaries,
        check.no_empty_runs,
        check.no_adjacent_same_kind,
        check.passed
    );
}

fn display_chunks_for_highlights(text: &str, highlights: &[Highlight]) -> Vec<DisplayTextChunk> {
    let buffer = TextBuffer::new(text);
    let mut display_map = DisplayMap::new();
    let folded_lines = HashSet::new();
    display_map.sync(buffer.line_count(), &[], &folded_lines);
    let snapshot = display_map.snapshot(
        buffer.snapshot(),
        &[],
        &folded_lines,
        false,
        Arc::new(highlights.to_vec()),
        &[],
        Arc::new(Vec::new()),
        &[],
    );
    snapshot.text_chunks(0..snapshot.display_line_count())
}

fn display_chunk_check(chunks: &[DisplayTextChunk]) -> DisplayChunkCheck {
    let mut check = DisplayChunkCheck {
        passed: true,
        chunk_count: chunks.len(),
        highlight_count: chunks.iter().map(|chunk| chunk.highlights.len()).sum(),
        chunks: chunks
            .iter()
            .map(|chunk| DisplayChunkSummary {
                buffer_line: chunk.buffer_line,
                start_offset: chunk.start_offset,
                text_len: chunk.text.len(),
                highlights: chunk
                    .highlights
                    .iter()
                    .map(|highlight| DisplayChunkHighlightSummary {
                        start: highlight.start,
                        end: highlight.end,
                        kind: highlight_kind_name(highlight.kind),
                    })
                    .collect(),
            })
            .collect(),
        no_empty_runs: true,
        no_overlaps: true,
        char_boundaries: true,
        failure: None,
    };

    for chunk in chunks {
        for (index, highlight) in chunk.highlights.iter().enumerate() {
            if highlight.start >= highlight.end {
                check.no_empty_runs = false;
                check.failure.get_or_insert_with(|| {
                    format!(
                        "display chunk line {} highlight {index} is empty",
                        chunk.buffer_line
                    )
                });
            }
            if highlight.end > chunk.text.len()
                || !chunk.text.is_char_boundary(highlight.start)
                || !chunk.text.is_char_boundary(highlight.end)
            {
                check.char_boundaries = false;
                check.failure.get_or_insert_with(|| {
                    format!(
                        "display chunk line {} highlight {index} has invalid UTF-8 boundaries {}..{}",
                        chunk.buffer_line, highlight.start, highlight.end
                    )
                });
            }
            if let Some(previous) = index
                .checked_sub(1)
                .and_then(|index| chunk.highlights.get(index))
                && previous.end > highlight.start
            {
                check.no_overlaps = false;
                check.failure.get_or_insert_with(|| {
                    format!(
                        "display chunk line {} has overlapping highlights {}..{} and {}..{}",
                        chunk.buffer_line,
                        previous.start,
                        previous.end,
                        highlight.start,
                        highlight.end
                    )
                });
            }
        }
    }

    check.passed = check.no_empty_runs && check.no_overlaps && check.char_boundaries;
    check
}

fn check_display_chunks(check: &DisplayChunkCheck) -> anyhow::Result<()> {
    if !check.passed {
        return Err(anyhow::anyhow!(
            "display chunk invariant failed: {}",
            check.failure.as_deref().unwrap_or("unknown failure")
        ));
    }

    Ok(())
}

fn print_display_chunk_check(check: &DisplayChunkCheck) {
    println!(
        "display_chunks chunks={} highlights={} no_empty_runs={} no_overlaps={} char_boundaries={} passed={}",
        check.chunk_count,
        check.highlight_count,
        check.no_empty_runs,
        check.no_overlaps,
        check.char_boundaries,
        check.passed
    );
}

fn check_render_invariants(check: &RenderInvariantCheck) -> anyhow::Result<()> {
    if !check.passed {
        return Err(anyhow::anyhow!(
            "render invariant failed: {}",
            check.failure.as_deref().unwrap_or("unknown failure")
        ));
    }

    Ok(())
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_term_smoke_check(driver: &str) -> anyhow::Result<Option<DriverTermSmokeCheck>> {
    let Some(metadata) = get_syntax_metadata_for_driver(driver) else {
        return Ok(None);
    };
    let profile = profile_for_driver(driver)?;
    if matches!(
        metadata.capabilities.overlays,
        zqlz_core::SyntaxOverlayMode::Command
    ) {
        return driver_command_term_smoke_check(driver, profile, &metadata.syntax_terms);
    }
    if matches!(
        metadata.capabilities.overlays,
        zqlz_core::SyntaxOverlayMode::Document
    ) {
        return Ok(None);
    }

    let terms = &metadata.syntax_terms;
    if terms.keywords.is_empty() && terms.functions.is_empty() && terms.types.is_empty() {
        return Ok(None);
    }

    let profile_terms = get_syntax_term_profile(profile);
    let mut type_like_terms = terms.types.clone();
    type_like_terms.extend(
        profile_terms
            .base_types
            .iter()
            .chain(profile_terms.dialect_types)
            .map(|term| (*term).to_string()),
    );
    let type_terms = terms.types.iter().take(4).cloned().collect::<Vec<_>>();
    let function_terms = select_non_overlapping_terms(&terms.functions, &type_like_terms, 4);
    let mut excluded_keywords = type_like_terms;
    excluded_keywords.extend(terms.functions.clone());
    let keyword_terms = select_non_overlapping_terms(&terms.keywords, &excluded_keywords, 4);

    let mut expected = Vec::new();
    let mut text = String::new();

    if !keyword_terms.is_empty() {
        text.push_str("SELECT 1;\n");
        for keyword in &keyword_terms {
            text.push_str(keyword);
            text.push_str(" sample_token;\n");
            expected.push((keyword.clone(), HighlightKind::Keyword));
        }
    }

    if !function_terms.is_empty() {
        text.push_str("SELECT ");
        for (index, function) in function_terms.iter().enumerate() {
            if index > 0 {
                text.push_str(", ");
            }
            text.push_str(function);
            text.push_str("(NULL)");
            expected.push((function.clone(), HighlightKind::Function));
        }
        text.push_str(";\n");
    }

    if !type_terms.is_empty() {
        for type_name in &type_terms {
            text.push_str("SELECT CAST(NULL AS ");
            text.push_str(type_name);
            text.push_str(");\n");
            expected.push((type_name.clone(), HighlightKind::Type));
        }
    }

    if expected.is_empty() {
        return Ok(None);
    }

    let mut highlighter = SyntaxHighlighter::new().map_err(anyhow::Error::msg)?;
    highlighter.set_language_profile(profile);
    apply_driver_syntax_metadata(&mut highlighter, driver)?;
    let highlights = highlighter.highlight(&text);
    let rendered_highlights = render_highlight_runs(&text, &highlights);
    let render_invariant_check = render_invariant_check(
        &text,
        &rendered_highlights,
        RenderInvariantMode::FullDocument,
    );
    check_render_invariants(&render_invariant_check)?;
    let expected_pairs = expected;
    let expected = expected_pairs
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds_for_token(&text, &highlights, token);
            let passed = assigned.contains(kind);
            DriverTermAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed = expected.iter().all(|check| check.passed);
    let render_expected = expected_pairs
        .iter()
        .map(|(token, kind)| {
            let assigned = painted_kinds_for_token(&text, &rendered_highlights, token);
            let passed = assigned.contains(kind);
            DriverTermAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let render_passed = render_expected.iter().all(|check| check.passed);

    if !passed || !render_passed {
        return Err(anyhow::anyhow!(
            "{profile} driver `{driver}` syntax term smoke failed: raw={:?} render={:?}",
            expected
                .iter()
                .filter(|check| !check.passed)
                .map(|check| (&check.token, check.kind, &check.assigned_kinds))
                .collect::<Vec<_>>(),
            render_expected
                .iter()
                .filter(|check| !check.passed)
                .map(|check| (&check.token, check.kind, &check.assigned_kinds))
                .collect::<Vec<_>>()
        ));
    }

    Ok(Some(DriverTermSmokeCheck {
        driver: driver.to_string(),
        profile,
        text,
        passed: passed && render_passed && render_invariant_check.passed,
        expected,
        render_expected,
        render_invariant_check,
    }))
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_command_term_smoke_check(
    driver: &str,
    profile: &'static str,
    terms: &zqlz_core::dialect_config::SyntaxTermsConfig,
) -> anyhow::Result<Option<DriverTermSmokeCheck>> {
    if terms.keywords.is_empty() {
        return Ok(None);
    }

    let preferred_terms = [
        "JSON.SET",
        "JSON.GET",
        "HSET",
        "ZRANGE",
        "WITHSCORES",
        "SET",
        "NX",
        "PX",
    ];
    let keyword_terms = preferred_terms
        .iter()
        .filter(|term| {
            terms
                .keywords
                .iter()
                .any(|keyword| keyword.eq_ignore_ascii_case(term))
        })
        .take(8)
        .map(|term| (*term).to_string())
        .collect::<Vec<_>>();

    if keyword_terms.is_empty() {
        return Ok(None);
    }

    let mut text = String::new();
    let mut expected = Vec::new();
    for keyword in &keyword_terms {
        match keyword.as_str() {
            "JSON.SET" => text.push_str("JSON.SET smoke:key $ \"{}\"\n"),
            "JSON.GET" => text.push_str("JSON.GET smoke:key $.path\n"),
            "HSET" => text.push_str("HSET smoke:hash field value\n"),
            "ZRANGE" => text.push_str("ZRANGE smoke:zset 0 1\n"),
            "SET" => text.push_str("SET smoke:key value\n"),
            "NX" | "PX" => {
                continue;
            }
            "WITHSCORES" => {
                continue;
            }
            _ => {
                text.push_str(keyword);
                text.push_str(" smoke:key\n");
            }
        }
        expected.push((keyword.clone(), HighlightKind::Keyword));
    }
    if keyword_terms.iter().any(|term| term == "WITHSCORES") {
        text.push_str("ZRANGE smoke:zset 0 1 WITHSCORES\n");
        expected.push(("WITHSCORES".to_string(), HighlightKind::Keyword));
    }
    if keyword_terms.iter().any(|term| term == "NX")
        || keyword_terms.iter().any(|term| term == "PX")
    {
        text.push_str("SET smoke:lock value NX PX 30000\n");
        if keyword_terms.iter().any(|term| term == "NX") {
            expected.push(("NX".to_string(), HighlightKind::Keyword));
        }
        if keyword_terms.iter().any(|term| term == "PX") {
            expected.push(("PX".to_string(), HighlightKind::Keyword));
        }
    }

    if expected.is_empty() {
        return Ok(None);
    }

    let mut highlighter = SyntaxHighlighter::new().map_err(anyhow::Error::msg)?;
    highlighter.set_language_profile(profile);
    apply_driver_syntax_metadata(&mut highlighter, driver)?;
    let highlights = highlighter.highlight(&text);
    let rendered_highlights = render_highlight_runs(&text, &highlights);
    let render_invariant_check = render_invariant_check(
        &text,
        &rendered_highlights,
        RenderInvariantMode::FullDocument,
    );
    check_render_invariants(&render_invariant_check)?;
    let expected_pairs = expected;
    let expected = expected_pairs
        .iter()
        .map(|(token, kind)| {
            let assigned = assigned_kinds_for_token(&text, &highlights, token);
            let passed = assigned.contains(kind);
            DriverTermAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let passed = expected.iter().all(|check| check.passed);
    let render_expected = expected_pairs
        .iter()
        .map(|(token, kind)| {
            let assigned = painted_kinds_for_token(&text, &rendered_highlights, token);
            let passed = assigned.contains(kind);
            DriverTermAssignmentCheck {
                token: token.clone(),
                kind: highlight_kind_name(*kind),
                assigned_kinds: assigned.into_iter().map(highlight_kind_name).collect(),
                passed,
            }
        })
        .collect::<Vec<_>>();
    let render_passed = render_expected.iter().all(|check| check.passed);

    if !passed || !render_passed {
        return Err(anyhow::anyhow!(
            "{profile} command driver `{driver}` syntax term smoke failed: raw={:?} render={:?}",
            expected
                .iter()
                .filter(|check| !check.passed)
                .map(|check| (&check.token, check.kind, &check.assigned_kinds))
                .collect::<Vec<_>>(),
            render_expected
                .iter()
                .filter(|check| !check.passed)
                .map(|check| (&check.token, check.kind, &check.assigned_kinds))
                .collect::<Vec<_>>()
        ));
    }

    Ok(Some(DriverTermSmokeCheck {
        driver: driver.to_string(),
        profile,
        text,
        passed: passed && render_passed && render_invariant_check.passed,
        expected,
        render_expected,
        render_invariant_check,
    }))
}

#[cfg(feature = "syntax-probe-drivers")]
fn select_non_overlapping_terms(
    terms: &[String],
    excluded_terms: &[String],
    limit: usize,
) -> Vec<String> {
    let excluded = excluded_terms
        .iter()
        .map(|term| term.to_ascii_uppercase())
        .collect::<HashSet<_>>();
    terms
        .iter()
        .filter(|term| !excluded.contains(&term.to_ascii_uppercase()))
        .take(limit)
        .cloned()
        .collect()
}

fn assigned_kinds_for_token(
    text: &str,
    highlights: &[Highlight],
    token: &str,
) -> Vec<HighlightKind> {
    let mut kinds = highlights
        .iter()
        .filter(|highlight| &text[highlight.start..highlight.end] == token)
        .map(|highlight| highlight.kind)
        .collect::<Vec<_>>();

    let mut search_start = 0usize;
    while let Some(relative_start) = text[search_start..].find(token) {
        let start = search_start + relative_start;
        let end = start + token.len();
        search_start = end;

        kinds.extend(
            highlights
                .iter()
                .filter(|highlight| highlight.start <= start && highlight.end >= end)
                .map(|highlight| highlight.kind),
        );
    }

    kinds.sort_by_key(|kind| highlight_kind_rank(*kind));
    kinds.dedup();
    kinds
}

fn painted_kinds_for_token(
    text: &str,
    highlights: &[Highlight],
    token: &str,
) -> Vec<HighlightKind> {
    let mut kinds = Vec::new();
    let mut search_start = 0usize;
    while let Some(relative_start) = text[search_start..].find(token) {
        let start = search_start + relative_start;
        let end = start + token.len();
        search_start = end;

        let covering = highlights
            .iter()
            .filter(|highlight| highlight.start < end && highlight.end > start)
            .collect::<Vec<_>>();
        if covering.is_empty() {
            continue;
        }

        let mut cursor = start;
        let mut token_kind = None;
        let mut covered = true;
        for highlight in covering {
            if highlight.start > cursor {
                covered = false;
                break;
            }
            if token_kind.is_none() {
                token_kind = Some(highlight.kind);
            } else if token_kind != Some(highlight.kind) {
                covered = false;
                break;
            }
            cursor = cursor.max(highlight.end.min(end));
            if cursor >= end {
                break;
            }
        }

        if covered
            && cursor >= end
            && let Some(kind) = token_kind
        {
            kinds.push(kind);
        }
    }

    kinds.sort_by_key(|kind| highlight_kind_rank(*kind));
    kinds.dedup();
    kinds
}

fn display_chunk_kinds_for_token(chunks: &[DisplayTextChunk], token: &str) -> Vec<HighlightKind> {
    let mut kinds = Vec::new();

    for chunk in chunks {
        let mut search_start = 0usize;
        while let Some(relative_start) = chunk.text[search_start..].find(token) {
            let start = search_start + relative_start;
            let end = start + token.len();
            search_start = end;

            let covering = chunk
                .highlights
                .iter()
                .filter(|highlight| highlight.start < end && highlight.end > start)
                .collect::<Vec<_>>();
            if covering.is_empty() {
                continue;
            }

            let mut cursor = start;
            let mut token_kind = None;
            let mut covered = true;
            for highlight in covering {
                if highlight.start > cursor {
                    covered = false;
                    break;
                }
                if token_kind.is_none() {
                    token_kind = Some(highlight.kind);
                } else if token_kind != Some(highlight.kind) {
                    covered = false;
                    break;
                }
                cursor = cursor.max(highlight.end.min(end));
            }
            if covered
                && cursor >= end
                && let Some(kind) = token_kind
            {
                kinds.push(kind);
            }
        }
    }

    kinds.sort_by_key(|kind| highlight_kind_rank(*kind));
    kinds.dedup();
    kinds
}

fn highlight_kind_rank(kind: HighlightKind) -> u8 {
    match kind {
        HighlightKind::Error => 0,
        HighlightKind::Comment => 1,
        HighlightKind::String => 2,
        HighlightKind::Parameter => 3,
        HighlightKind::Keyword => 4,
        HighlightKind::Function => 5,
        HighlightKind::Type => 6,
        HighlightKind::Operator => 7,
        HighlightKind::Identifier => 8,
        HighlightKind::Number => 9,
        HighlightKind::Boolean => 10,
        HighlightKind::Null => 11,
        HighlightKind::Punctuation => 12,
        HighlightKind::Default => 13,
    }
}

fn profile_for_driver(driver: &str) -> anyhow::Result<&'static str> {
    #[cfg(not(feature = "syntax-probe-drivers"))]
    {
        let _ = driver;
        return Err(anyhow::anyhow!(
            "`--driver` requires `syntax-probe-drivers` or `syntax-probe-all-drivers` feature"
        ));
    }

    #[cfg(feature = "syntax-probe-drivers")]
    {
        get_syntax_metadata_for_driver(driver)
            .map(|metadata| metadata.capabilities.profile)
            .ok_or_else(|| anyhow::anyhow!("unknown driver or missing syntax metadata: {driver}"))
    }
}

#[cfg(feature = "syntax-probe-drivers")]
fn driver_syntax_quality_fixture(
    driver: &str,
    profile: &'static str,
) -> anyhow::Result<ProbeSyntaxQualityFixture> {
    let metadata = get_syntax_metadata_for_driver(driver)
        .ok_or_else(|| anyhow::anyhow!("unknown driver or missing syntax metadata: {driver}"))?;
    let text = metadata.syntax_quality.text.ok_or_else(|| {
        anyhow::anyhow!("missing driver-owned syntax quality fixture for `{driver}`")
    })?;
    if metadata.syntax_quality.expected.is_empty() {
        return Err(anyhow::anyhow!(
            "driver-owned syntax quality fixture for `{driver}` has no expected assignments"
        ));
    }
    let expected_tokens = metadata
        .syntax_quality
        .expected
        .into_iter()
        .map(|token| Ok((token.token, highlight_kind_from_name(&token.kind)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    let rejected_tokens = metadata
        .syntax_quality
        .rejected
        .into_iter()
        .map(|token| Ok((token.token, highlight_kind_from_name(&token.kind)?)))
        .collect::<anyhow::Result<Vec<_>>>()?;
    if metadata.syntax_quality.expected_outline.is_empty() {
        return Err(anyhow::anyhow!(
            "driver-owned syntax quality fixture for `{driver}` has no expected outline symbols"
        ));
    }

    Ok(ProbeSyntaxQualityFixture {
        profile,
        source: "driver-bundle",
        text,
        expected_tokens,
        rejected_tokens,
        expected_outline: metadata.syntax_quality.expected_outline,
    })
}

fn apply_driver_syntax_metadata(
    highlighter: &mut SyntaxHighlighter,
    driver: &str,
) -> anyhow::Result<()> {
    #[cfg(not(feature = "syntax-probe-drivers"))]
    {
        let _ = (highlighter, driver);
        Ok(())
    }

    #[cfg(feature = "syntax-probe-drivers")]
    {
        let Some(metadata) = get_syntax_metadata_for_driver(driver) else {
            return Err(anyhow::anyhow!(
                "unknown driver or missing syntax metadata: {driver}"
            ));
        };
        highlighter.set_syntax_capabilities_override(metadata.capabilities);
        if let Some(terms) =
            zqlz_text_editor::syntax::SyntaxTermOverrides::from_config(&metadata.syntax_terms)
        {
            highlighter.set_driver_syntax_terms(terms);
        }
        Ok(())
    }
}

fn check_fail_under(coverage: &HighlightCoverage, fail_under: Option<f64>) -> anyhow::Result<()> {
    if let Some(threshold) = fail_under
        && coverage.percent < threshold
    {
        return Err(anyhow::anyhow!(
            "highlight coverage {:.2}% is below {:.2}%",
            coverage.percent,
            threshold
        ));
    }
    Ok(())
}

fn check_timing_under(timing: &HighlightTiming, fail_avg_ms: Option<f64>) -> anyhow::Result<()> {
    if let Some(threshold) = fail_avg_ms
        && timing.avg_ms > threshold
    {
        return Err(anyhow::anyhow!(
            "highlight avg {:.3}ms is above {:.3}ms",
            timing.avg_ms,
            threshold
        ));
    }
    Ok(())
}

fn check_phase_timing_under(
    timing: &HighlightTiming,
    fail_phase_avg_ms: Option<f64>,
) -> anyhow::Result<()> {
    let Some(threshold) = fail_phase_avg_ms else {
        return Ok(());
    };

    for phase in &timing.phases {
        if phase.avg_ms > threshold {
            return Err(anyhow::anyhow!(
                "highlight phase `{}` avg {:.3}ms is above {:.3}ms",
                phase.name,
                phase.avg_ms,
                threshold
            ));
        }
    }

    Ok(())
}

fn debug_token(text: &str) -> String {
    text.escape_debug().to_string()
}

fn highlight_kind_name(kind: HighlightKind) -> &'static str {
    match kind {
        HighlightKind::Keyword => "Keyword",
        HighlightKind::String => "String",
        HighlightKind::Comment => "Comment",
        HighlightKind::Number => "Number",
        HighlightKind::Identifier => "Identifier",
        HighlightKind::Operator => "Operator",
        HighlightKind::Function => "Function",
        HighlightKind::Parameter => "Parameter",
        HighlightKind::Type => "Type",
        HighlightKind::Punctuation => "Punctuation",
        HighlightKind::Boolean => "Boolean",
        HighlightKind::Null => "Null",
        HighlightKind::Error => "Error",
        HighlightKind::Default => "Default",
    }
}

fn highlight_kind_from_name(kind: &str) -> anyhow::Result<HighlightKind> {
    let normalized = kind.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "keyword" => Ok(HighlightKind::Keyword),
        "string" => Ok(HighlightKind::String),
        "comment" => Ok(HighlightKind::Comment),
        "number" => Ok(HighlightKind::Number),
        "identifier" => Ok(HighlightKind::Identifier),
        "operator" => Ok(HighlightKind::Operator),
        "function" => Ok(HighlightKind::Function),
        "parameter" => Ok(HighlightKind::Parameter),
        "type" => Ok(HighlightKind::Type),
        "punctuation" => Ok(HighlightKind::Punctuation),
        "boolean" => Ok(HighlightKind::Boolean),
        "null" => Ok(HighlightKind::Null),
        "error" => Ok(HighlightKind::Error),
        "default" => Ok(HighlightKind::Default),
        _ => Err(anyhow::anyhow!("unknown highlight kind: {kind}")),
    }
}

fn print_help() {
    println!(
        "Usage: syntax_assignment_probe [--json] [--coverage] [--fail-under PERCENT] [--profile PROFILE] (--text SQL | --file PATH)\n\
         Usage: syntax_assignment_probe [--json] [--coverage] [--fail-under PERCENT] [--driver DRIVER] (--text SQL | --file PATH)\n\
         Usage: syntax_assignment_probe [--json] [--summary] --outline (--text SQL | --file PATH)\n\
         Usage: syntax_assignment_probe [--json] [--summary] [--profile PROFILE|--driver DRIVER] --execution-unit [--cursor BYTE] [--expect-unit TEXT] [--expect-unit-range START:END] (--text SQL | --file PATH)\n\
         Usage: syntax_assignment_probe [--json] [--summary] [--profile PROFILE|--driver DRIVER] --editor-op (toggle-comment|toggle-block-comment|newline|auto-close) [--cursor BYTE] [--selection-start BYTE --selection-end BYTE] (--text SQL | --file PATH)\n\
         Usage: syntax_assignment_probe [--json] [--summary] [--profile PROFILE|--driver DRIVER] --capabilities [--expect-capability KEY=VALUE]\n\
         Usage: syntax_assignment_probe [--json] [--coverage] [--profile PROFILE] --generated-large-sql STATEMENTS\n\
         Usage: syntax_assignment_probe [--json] [--fail-under PERCENT] --fixture (all|PROFILE)\n\
         Usage: syntax_assignment_probe [--json] [--fail-under PERCENT] --driver-fixture (all|DRIVER)\n\
         Options: [--summary] [--outline] [--brackets --bracket-cursor BYTE] [--execution-unit --cursor BYTE] [--editor-op OP --cursor BYTE] [--capabilities] [--render-runs] [--token-report TOKEN] [--full-rope] [--timing] [--phase-timing] [--repeat N] [--fail-avg-ms MS] [--fail-phase-avg-ms MS] [--fail-render-avg-ms MS] [--viewport-start BYTE --viewport-bytes BYTES]\n\
         Checks: [--expect TOKEN:KIND] [--reject TOKEN:KIND] [--expect-render TOKEN:KIND] [--reject-render TOKEN:KIND]\n\
         Outline checks: [--expect-symbol LABEL]\n\
         Editor checks: [--expect-edit START:END:TEXT] (TEXT supports \\n and \\t escapes)\n\
         Capability checks: [--expect-capability KEY=VALUE]\n\
         Execution-unit checks: [--expect-unit TEXT] [--expect-unit-range START:END]\n\
         Without --text/--file, SQL is read from stdin."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn highlight_kind_names_match_debug_labels() {
        assert_eq!(highlight_kind_name(HighlightKind::Keyword), "Keyword");
        assert_eq!(highlight_kind_name(HighlightKind::Parameter), "Parameter");
        assert_eq!(
            highlight_kind_name(HighlightKind::Punctuation),
            "Punctuation"
        );
    }

    #[test]
    fn coverage_reports_unhighlighted_non_whitespace_gaps() {
        let text = "SELECT custom_token FROM t";
        let highlights = vec![
            Highlight {
                start: 0,
                end: 6,
                kind: HighlightKind::Keyword,
            },
            Highlight {
                start: 20,
                end: 24,
                kind: HighlightKind::Keyword,
            },
        ];
        let coverage = highlight_coverage(text, &highlights);

        assert_eq!(coverage.styled_non_ws_bytes, 10);
        assert_eq!(coverage.non_ws_bytes, 23);
        assert_eq!(coverage.gaps.len(), 2);
        assert_eq!(coverage.gaps[0].text, "custom_token");
        assert_eq!(coverage.gaps[1].text, "t");
    }

    #[test]
    fn fixture_probe_lists_all_supported_quality_fixtures() {
        let profiles = syntax_quality_fixtures()
            .iter()
            .map(|fixture| fixture.profile)
            .collect::<Vec<_>>();

        assert_eq!(
            profiles,
            vec![
                "postgresql",
                "mysql",
                "sqlite",
                "duckdb",
                "mssql",
                "clickhouse",
                "mongodb",
                "redis"
            ]
        );
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn profile_for_driver_uses_driver_bundle_aliases() {
        assert_eq!(profile_for_driver(" PostgreSQL ").unwrap(), "postgresql");
        assert_eq!(profile_for_driver("mariadb").unwrap(), "mysql");
        assert_eq!(profile_for_driver("mongo").unwrap(), "mongodb");
        assert_eq!(profile_for_driver("turso").unwrap(), "sqlite");
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn driver_fixture_probe_requires_driver_bundle_source() {
        for driver in DriverRegistry::with_defaults().list() {
            let profile = profile_for_driver(driver).expect("driver profile");
            let fixture = driver_syntax_quality_fixture(driver, profile).expect("driver fixture");
            assert_eq!(fixture.source, "driver-bundle", "{driver}");
            assert!(
                !fixture.expected_tokens.is_empty(),
                "{driver} should expose expected syntax assignments"
            );
        }
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn capabilities_probe_reports_driver_bundle_metadata() {
        let postgres = capabilities_probe_result(
            "postgresql",
            Some("postgres"),
            &[
                ExpectedCapability {
                    key: "profile".to_string(),
                    value: "postgresql".to_string(),
                },
                ExpectedCapability {
                    key: "source".to_string(),
                    value: "driver-bundle".to_string(),
                },
                ExpectedCapability {
                    key: "brackets".to_string(),
                    value: "TreeSitter".to_string(),
                },
                ExpectedCapability {
                    key: "line_comment_prefix".to_string(),
                    value: "--".to_string(),
                },
                ExpectedCapability {
                    key: "sql_overlays".to_string(),
                    value: "true".to_string(),
                },
                ExpectedCapability {
                    key: "formatter".to_string(),
                    value: "Sql".to_string(),
                },
                ExpectedCapability {
                    key: "parameter_placeholders_enabled".to_string(),
                    value: "true".to_string(),
                },
                ExpectedCapability {
                    key: "parameter_placeholders_question_mark".to_string(),
                    value: "false".to_string(),
                },
                ExpectedCapability {
                    key: "execution_unit".to_string(),
                    value: "SqlStatement".to_string(),
                },
                ExpectedCapability {
                    key: "document_symbols".to_string(),
                    value: "SqlStatements".to_string(),
                },
                ExpectedCapability {
                    key: "overlays".to_string(),
                    value: "Sql".to_string(),
                },
                ExpectedCapability {
                    key: "markdown_fence_language".to_string(),
                    value: "sql".to_string(),
                },
                ExpectedCapability {
                    key: "completion_triggers".to_string(),
                    value: ". (,".to_string(),
                },
                ExpectedCapability {
                    key: "completion_word_chars".to_string(),
                    value: "$".to_string(),
                },
            ],
        )
        .expect("postgres capabilities");
        assert!(postgres.check.passed);

        let redis = capabilities_probe_result(
            "redis",
            Some("redis"),
            &[
                ExpectedCapability {
                    key: "command_syntax".to_string(),
                    value: "true".to_string(),
                },
                ExpectedCapability {
                    key: "line_comment_prefix".to_string(),
                    value: "#".to_string(),
                },
                ExpectedCapability {
                    key: "brackets".to_string(),
                    value: "Standard".to_string(),
                },
                ExpectedCapability {
                    key: "formatter".to_string(),
                    value: "Custom".to_string(),
                },
                ExpectedCapability {
                    key: "parameter_placeholders_enabled".to_string(),
                    value: "false".to_string(),
                },
                ExpectedCapability {
                    key: "execution_unit".to_string(),
                    value: "CommandLine".to_string(),
                },
                ExpectedCapability {
                    key: "document_symbols".to_string(),
                    value: "CommandCommands".to_string(),
                },
                ExpectedCapability {
                    key: "overlays".to_string(),
                    value: "Command".to_string(),
                },
                ExpectedCapability {
                    key: "markdown_fence_language".to_string(),
                    value: "redis".to_string(),
                },
                ExpectedCapability {
                    key: "completion_triggers".to_string(),
                    value: " ".to_string(),
                },
                ExpectedCapability {
                    key: "completion_word_chars".to_string(),
                    value: ":-".to_string(),
                },
            ],
        )
        .expect("redis capabilities");
        assert!(redis.check.passed);

        let mongo = capabilities_probe_result(
            "mongodb",
            Some("mongo"),
            &[
                ExpectedCapability {
                    key: "profile".to_string(),
                    value: "mongodb".to_string(),
                },
                ExpectedCapability {
                    key: "document_syntax".to_string(),
                    value: "true".to_string(),
                },
                ExpectedCapability {
                    key: "formatter".to_string(),
                    value: "Custom".to_string(),
                },
                ExpectedCapability {
                    key: "tree_sitter_grammar".to_string(),
                    value: "Javascript".to_string(),
                },
                ExpectedCapability {
                    key: "execution_unit".to_string(),
                    value: "WholeDocument".to_string(),
                },
                ExpectedCapability {
                    key: "document_symbols".to_string(),
                    value: "MongodbCollections".to_string(),
                },
                ExpectedCapability {
                    key: "overlays".to_string(),
                    value: "Document".to_string(),
                },
                ExpectedCapability {
                    key: "markdown_fence_language".to_string(),
                    value: "javascript".to_string(),
                },
                ExpectedCapability {
                    key: "line_comment_prefix".to_string(),
                    value: "//".to_string(),
                },
                ExpectedCapability {
                    key: "completion_triggers".to_string(),
                    value: ".:{[,".to_string(),
                },
                ExpectedCapability {
                    key: "completion_word_chars".to_string(),
                    value: "$".to_string(),
                },
            ],
        )
        .expect("mongo capabilities");
        assert!(mongo.check.passed);
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn apply_driver_syntax_metadata_applies_capabilities_and_terms() {
        let mut highlighter = SyntaxHighlighter::new().expect("highlighter");
        assert_eq!(highlighter.language_profile(), "sql");

        apply_driver_syntax_metadata(&mut highlighter, "mongo").expect("mongo metadata");
        let text = "db.users.find({ _id: ObjectId(\"abc\") })";
        let highlights = highlighter.highlight(text);

        assert_eq!(highlighter.language_profile(), "sql");
        assert!(
            assigned_kinds_for_token(text, &highlights, "find").contains(&HighlightKind::Function)
        );
        assert!(
            assigned_kinds_for_token(text, &highlights, "ObjectId").contains(&HighlightKind::Type)
        );
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn registry_driver_capability_contracts_cover_all_default_drivers() {
        let mut drivers = DriverRegistry::with_defaults()
            .list()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        drivers.sort_unstable();

        assert!(!drivers.is_empty());
        for driver in drivers {
            let profile = profile_for_driver(&driver).expect("driver profile");
            let check =
                driver_capability_contract_check(&driver, profile).expect("capability contract");
            assert!(
                check.passed,
                "driver `{driver}` profile `{profile}` failed capability contract: {:?}",
                check
                    .expected
                    .iter()
                    .filter(|expected| !expected.matched)
                    .collect::<Vec<_>>()
            );
        }
    }

    #[cfg(feature = "syntax-probe-drivers")]
    #[test]
    fn registry_driver_completion_contracts_cover_all_default_drivers() {
        let mut drivers = DriverRegistry::with_defaults()
            .list()
            .into_iter()
            .map(str::to_string)
            .collect::<Vec<_>>();
        drivers.sort_unstable();

        assert!(!drivers.is_empty());
        for driver in drivers {
            let check = driver_completion_contract_check(&driver)
                .expect("driver completion contract should run");
            assert!(
                check.passed,
                "driver `{driver}` failed completion contract: expected_label={:?} prefix={:?} fallback_prefix={:?} fallback={:?} triggers={:?} word_chars={:?} metadata={:?}",
                check.expected_label,
                check.prefix,
                check.fallback_prefix,
                check.fallback_matched_labels,
                check.trigger_checks,
                check.word_char_checks,
                check.metadata_checks
            );
            assert_eq!(check.prefix, check.fallback_prefix);
            assert!(
                check
                    .expected_label
                    .as_ref()
                    .is_some_and(|label| check.fallback_matched_labels.contains(label))
            );
            assert!(check.trigger_checks.iter().all(|trigger| trigger.passed));
            assert!(
                check
                    .word_char_checks
                    .iter()
                    .all(|word_char| word_char.passed)
            );
            assert!(check.metadata_checks.iter().all(|metadata| metadata.passed));
        }
    }

    #[test]
    fn capability_checker_rejects_wrong_value() {
        let result = capabilities_probe_result(
            "redis",
            None,
            &[ExpectedCapability {
                key: "line_comment_prefix".to_string(),
                value: "--".to_string(),
            }],
        )
        .expect("redis capabilities");

        assert!(!result.check.passed);
        assert!(check_capabilities(&result.check).is_err());
        assert_eq!(
            capability_value(&result, "line_comment_prefix").as_deref(),
            Some("<none>")
        );
        assert_eq!(capability_value(&result, "unknown"), None);
    }

    #[test]
    fn fixture_assignment_checker_rejects_wrong_kind() {
        let fixture = syntax_quality_fixtures()
            .iter()
            .copied()
            .find(|fixture| fixture.profile == "postgresql")
            .expect("postgres fixture");
        let token = fixture.expected_tokens[0].0;
        let start = fixture.text.find(token).expect("expected token in fixture");
        let highlights = vec![Highlight {
            start,
            end: start + token.len(),
            kind: HighlightKind::Identifier,
        }];
        let assignment_check = fixture_assignment_check(&fixture, &highlights);

        let error = check_fixture_assignments(&fixture, &assignment_check)
            .expect_err("wrong assignment should fail");
        assert!(error.to_string().contains("expected"));
    }

    #[test]
    fn token_assignment_report_tracks_raw_and_rendered_kinds() {
        let text = "CREATE TABLE \"categories\" (\"category_id\" text)";
        let token_start = text.find("\"category_id\"").expect("token");
        let type_start = text.find("text").expect("type token");
        let raw_highlights = vec![
            Highlight {
                start: token_start,
                end: token_start + "\"category_id\"".len(),
                kind: HighlightKind::Identifier,
            },
            Highlight {
                start: type_start,
                end: type_start + "text".len(),
                kind: HighlightKind::Type,
            },
        ];
        let rendered_highlights = render_highlight_runs(text, &raw_highlights);

        let report = token_assignment_report(
            text,
            &raw_highlights,
            &rendered_highlights,
            "\"category_id\"",
        );

        assert_eq!(report.occurrences.len(), 1);
        assert_eq!(report.occurrences[0].line, 1);
        assert_eq!(
            report.occurrences[0].raw_assigned_kinds,
            vec![highlight_kind_name(HighlightKind::Identifier)]
        );
        assert_eq!(
            report.occurrences[0].render_assigned_kinds,
            vec![highlight_kind_name(HighlightKind::Identifier)]
        );
    }

    #[test]
    fn parses_ad_hoc_assignment_expectations() {
        let expectation = parse_assignment_arg("CHECK:Keyword").expect("expectation");

        assert_eq!(expectation.token, "CHECK");
        assert_eq!(expectation.kind, HighlightKind::Keyword);
        assert!(parse_assignment_arg("CHECK:Nope").is_err());
    }

    #[test]
    fn editor_op_probe_reports_profile_comment_toggle_edits() {
        let text = "  db.categories.find()\nGET key";
        let result = editor_op_probe_result(EditorOpProbeRequest {
            text,
            profile: "mongodb",
            op: EditorProbeOp::ToggleComment,
            cursor_offset: 0,
            selection_start: Some(0),
            selection_end: Some(2),
            line_comment_prefix_override: None,
            expected_edits: &[],
        })
        .expect("editor op probe");

        assert_eq!(result.line_comment_prefix, Some("//"));
        assert_eq!(result.op, "toggle-comment");
        assert_eq!(result.edits.len(), 1);
        assert_eq!(result.edits[0].start, 2);
        assert_eq!(result.edits[0].end, 2);
        assert_eq!(result.edits[0].replacement, "// ");
    }

    #[test]
    fn editor_op_probe_reports_profile_newline_comment_continuation() {
        let text = "  -- explain plan";
        let result = editor_op_probe_result(EditorOpProbeRequest {
            text,
            profile: "postgresql",
            op: EditorProbeOp::Newline,
            cursor_offset: text.len(),
            selection_start: None,
            selection_end: None,
            line_comment_prefix_override: None,
            expected_edits: &[],
        })
        .expect("editor op probe");

        assert_eq!(result.line_comment_prefix, Some("--"));
        assert_eq!(result.op, "newline");
        assert_eq!(result.edits.len(), 1);
        assert_eq!(result.edits[0].start, text.len());
        assert_eq!(result.edits[0].replacement, "\n  -- ");
    }

    #[test]
    fn editor_op_probe_reports_delimiter_aware_newline_indent() {
        let text = "db.users.find({";
        let result = editor_op_probe_result(EditorOpProbeRequest {
            text,
            profile: "mongodb",
            op: EditorProbeOp::Newline,
            cursor_offset: text.len(),
            selection_start: None,
            selection_end: None,
            line_comment_prefix_override: None,
            expected_edits: &[ExpectedEditorEdit {
                start: text.len(),
                end: text.len(),
                replacement: "\n    ".to_string(),
            }],
        })
        .expect("editor op probe");

        assert!(result.edit_check.passed);
        assert_eq!(result.edits[0].replacement, "\n    ");
    }

    #[test]
    fn editor_op_probe_noops_comment_toggle_when_profile_has_no_line_comments() {
        let result = editor_op_probe_result(EditorOpProbeRequest {
            text: "GET key",
            profile: "redis",
            op: EditorProbeOp::ToggleComment,
            cursor_offset: 0,
            selection_start: None,
            selection_end: None,
            line_comment_prefix_override: Some(None),
            expected_edits: &[],
        })
        .expect("editor op probe");

        assert_eq!(result.line_comment_prefix, None);
        assert!(result.edits.is_empty());
    }

    #[test]
    fn editor_edit_checker_validates_expected_edits() {
        let expected = vec![ExpectedEditorEdit {
            start: 2,
            end: 2,
            replacement: "// ".to_string(),
        }];
        let result = editor_op_probe_result(EditorOpProbeRequest {
            text: "  db.categories.find()",
            profile: "mongodb",
            op: EditorProbeOp::ToggleComment,
            cursor_offset: 0,
            selection_start: Some(0),
            selection_end: Some(2),
            line_comment_prefix_override: None,
            expected_edits: &expected,
        })
        .expect("editor op probe");

        assert!(result.edit_check.passed);
        assert!(check_editor_edits(&result.edit_check).is_ok());

        let missing = EditorEditCheck {
            passed: false,
            expected: vec![ExpectedEditorEditCheck {
                start: 0,
                end: 0,
                replacement: "-- ".to_string(),
                matched: false,
            }],
        };
        assert!(check_editor_edits(&missing).is_err());
    }

    #[test]
    fn execution_unit_probe_reports_sql_command_and_document_units() {
        let sql = "select 1;\nselect 2;";
        let sql_result = execution_unit_probe_result(ExecutionUnitProbeRequest {
            text: sql,
            profile: "postgresql",
            cursor_offset: sql.find('2').expect("second statement"),
            expected_unit: Some("select 2"),
            expected_range: Some(sql.find("select 2").expect("second select")..sql.len() - 1),
        });
        assert!(sql_result.check.passed);
        assert!(sql_result.sql_span);

        let redis = "GET user:1\nSET user:1 Ada";
        let redis_result = execution_unit_probe_result(ExecutionUnitProbeRequest {
            text: redis,
            profile: "redis",
            cursor_offset: redis.find("Ada").expect("value"),
            expected_unit: Some("SET user:1 Ada"),
            expected_range: Some(redis.find("SET").expect("set")..redis.len()),
        });
        assert!(redis_result.check.passed);
        assert!(!redis_result.sql_span);

        let mongo = "db.users.find({ active: true })\ndb.users.countDocuments()";
        let mongo_result = execution_unit_probe_result(ExecutionUnitProbeRequest {
            text: mongo,
            profile: "mongodb",
            cursor_offset: mongo.find("count").expect("count"),
            expected_unit: Some(mongo),
            expected_range: Some(0..mongo.len()),
        });
        assert!(mongo_result.check.passed);
        assert!(!mongo_result.sql_span);
    }

    #[test]
    fn execution_unit_checker_rejects_wrong_expectations() {
        let result = execution_unit_probe_result(ExecutionUnitProbeRequest {
            text: "GET user:1",
            profile: "redis",
            cursor_offset: 0,
            expected_unit: Some("SET user:1 Ada"),
            expected_range: None,
        });

        assert!(!result.check.passed);
        assert!(check_execution_unit(&result.check).is_err());
    }

    #[test]
    fn ad_hoc_assignment_checker_reports_expected_and_rejected_tokens() {
        let text = "CHECK amount";
        let highlights = vec![Highlight {
            start: 0,
            end: 5,
            kind: HighlightKind::Keyword,
        }];
        let expected = vec![AssignmentExpectation {
            token: "CHECK".to_string(),
            kind: HighlightKind::Keyword,
        }];
        let rejected = vec![AssignmentExpectation {
            token: "amount".to_string(),
            kind: HighlightKind::Keyword,
        }];

        let check = ad_hoc_assignment_check(text, &highlights, &expected, &rejected);

        assert!(check.passed);
        assert_eq!(check.expected[0].assigned_kinds, vec!["Keyword"]);
        assert!(check.rejected[0].assigned_kinds.is_empty());
    }

    #[test]
    fn render_assignment_checker_uses_final_painted_kind() {
        let text = r#""categories""#;
        let highlights = vec![
            Highlight {
                start: 0,
                end: text.len(),
                kind: HighlightKind::String,
            },
            Highlight {
                start: 0,
                end: text.len(),
                kind: HighlightKind::Identifier,
            },
        ];
        let rendered_highlights = render_highlight_runs(text, &highlights);
        let check = ad_hoc_assignment_check(
            text,
            &rendered_highlights,
            &[AssignmentExpectation {
                token: text.to_string(),
                kind: HighlightKind::Identifier,
            }],
            &[AssignmentExpectation {
                token: text.to_string(),
                kind: HighlightKind::String,
            }],
        );

        assert!(check.passed);
        assert_eq!(rendered_highlights.len(), 1);
        assert_eq!(rendered_highlights[0].kind, HighlightKind::Identifier);
    }

    #[test]
    fn measure_highlight_reports_repeat_average() {
        let mut highlighter = SyntaxHighlighter::new().expect("highlighter");
        let (highlights, timing) = measure_highlight(&mut highlighter, "SELECT 1", 3, false);

        assert_eq!(timing.repeat, 3);
        assert!(timing.total_ms >= 0.0);
        assert_eq!(timing.avg_ms, timing.total_ms / 3.0);
        assert!(!highlights.is_empty());
        assert!(check_timing_under(&timing, Some(f64::MAX)).is_ok());
        assert!(
            check_timing_under(
                &HighlightTiming {
                    repeat: 1,
                    total_ms: 2.0,
                    avg_ms: 2.0,
                    phases: Vec::new(),
                },
                Some(1.0),
            )
            .is_err()
        );
    }

    #[test]
    fn measure_highlight_can_report_phase_timing() {
        let mut highlighter = SyntaxHighlighter::new().expect("highlighter");
        let (_highlights, timing) = measure_highlight(
            &mut highlighter,
            "SELECT jsonb_extract_path_text(payload, 'name')",
            2,
            true,
        );

        assert!(timing.phases.iter().any(|phase| phase.name == "overlays"));
        assert!(timing.phases.iter().any(|phase| phase.name == "normalize"));
        assert!(timing.phases.iter().all(|phase| phase.avg_ms >= 0.0));
        assert!(check_phase_timing_under(&timing, Some(f64::MAX)).is_ok());
        assert!(
            check_phase_timing_under(
                &HighlightTiming {
                    repeat: 1,
                    total_ms: 2.0,
                    avg_ms: 2.0,
                    phases: vec![HighlightPhaseTiming {
                        name: "overlays",
                        total_ms: 2.0,
                        avg_ms: 2.0,
                    }],
                },
                Some(1.0),
            )
            .is_err()
        );
    }

    #[test]
    fn measure_render_highlight_runs_reports_repeat_average() {
        let text = "SELECT \"category_id\"::text;";
        let highlights = vec![
            Highlight {
                start: 0,
                end: 6,
                kind: HighlightKind::Keyword,
            },
            Highlight {
                start: 7,
                end: 20,
                kind: HighlightKind::Identifier,
            },
            Highlight {
                start: 22,
                end: 26,
                kind: HighlightKind::Type,
            },
        ];

        let profile = measure_render_highlight_runs(text, &highlights, 3);

        assert_eq!(profile.timing.repeat, 3);
        assert_eq!(profile.timing.avg_ms, profile.timing.total_ms / 3.0);
        assert!(profile.timing.phases.is_empty());
        assert!(profile.highlights.len() >= highlights.len());
        assert!(check_timing_under(&profile.timing, Some(f64::MAX)).is_ok());
    }

    #[test]
    fn render_invariant_checker_accepts_full_contiguous_runs() {
        let text = "SELECT 1";
        let check = render_invariant_check(
            text,
            &[
                Highlight {
                    start: 0,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
                Highlight {
                    start: 6,
                    end: 7,
                    kind: HighlightKind::Default,
                },
                Highlight {
                    start: 7,
                    end: 8,
                    kind: HighlightKind::Number,
                },
            ],
            RenderInvariantMode::FullDocument,
        );

        assert!(check.passed);
        assert!(check_render_invariants(&check).is_ok());
    }

    #[test]
    fn render_invariant_checker_rejects_gaps_and_duplicate_neighbors() {
        let text = "SELECT 1";
        let check = render_invariant_check(
            text,
            &[
                Highlight {
                    start: 0,
                    end: 3,
                    kind: HighlightKind::Keyword,
                },
                Highlight {
                    start: 4,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
            ],
            RenderInvariantMode::FullDocument,
        );

        assert!(!check.passed);
        assert!(!check.contiguous);
        assert!(!check.no_adjacent_same_kind);
        assert!(check_render_invariants(&check).is_err());
    }

    #[test]
    fn display_chunk_checker_accepts_flattened_chunks() {
        let text = "SELECT name";
        let chunks = display_chunks_for_highlights(
            text,
            &[
                Highlight {
                    start: 0,
                    end: 6,
                    kind: HighlightKind::Keyword,
                },
                Highlight {
                    start: 7,
                    end: 11,
                    kind: HighlightKind::Identifier,
                },
            ],
        );

        let check = display_chunk_check(&chunks);
        assert!(check.passed);
        assert_eq!(check.chunk_count, 1);
        assert_eq!(
            display_chunk_kinds_for_token(&chunks, "SELECT"),
            vec![HighlightKind::Keyword]
        );
        assert_eq!(
            display_chunk_kinds_for_token(&chunks, "name"),
            vec![HighlightKind::Identifier]
        );
        assert!(check_display_chunks(&check).is_ok());
    }

    #[test]
    fn display_chunk_checker_rejects_manual_overlaps() {
        let chunks = vec![DisplayTextChunk {
            row_id: zqlz_text_editor::DisplayRowId {
                buffer_line: 0,
                wrap_subrow: 0,
            },
            display_row: 0,
            buffer_line: 0,
            start_offset: 0,
            text: "COUNT(*)".to_string(),
            highlights: vec![
                zqlz_text_editor::display_map::ChunkHighlight {
                    start: 0,
                    end: 5,
                    kind: HighlightKind::Function,
                },
                zqlz_text_editor::display_map::ChunkHighlight {
                    start: 0,
                    end: 8,
                    kind: HighlightKind::Identifier,
                },
            ],
            diagnostics: Vec::new(),
            inlay_hints: Vec::new(),
        }];

        let check = display_chunk_check(&chunks);
        assert!(!check.passed);
        assert!(!check.no_overlaps);
        assert!(check_display_chunks(&check).is_err());
    }

    #[test]
    fn outline_target_suffix_reports_target_range() {
        let symbol = SqlDocumentSymbol {
            label: "CREATE audit_log".to_string(),
            line: 0,
            column: 0,
            source_range: 0..31,
            target_range: Some(13..22),
        };

        assert_eq!(outline_target_suffix(&symbol), " target=13..22");
    }

    #[test]
    fn outline_probe_uses_core_sql_document_symbols() {
        let sql = "create or replace view reporting.monthly_sales as select 1;\n\
                   drop table if exists stale_sales;";
        let symbols = driver_document_symbols("postgresql", sql);

        assert_eq!(symbols.len(), 2);
        assert_eq!(symbols[0].label, "CREATE reporting.monthly_sales");
        assert_eq!(symbols[1].label, "DROP stale_sales");
        assert_eq!(
            &sql[symbols[0].target_range.clone().expect("target range")],
            "reporting.monthly_sales"
        );
    }

    #[test]
    fn outline_symbol_checker_reports_expected_labels() {
        let sql = "create or replace view reporting.monthly_sales as select 1;\n\
                   drop table if exists stale_sales;";
        let symbols = driver_document_symbols("postgresql", sql);
        let check = outline_symbol_check(
            &symbols,
            &[
                "CREATE reporting.monthly_sales".to_string(),
                "DROP stale_sales".to_string(),
            ],
        );

        assert!(check.passed);
        assert_eq!(check.expected[0].line, Some(0));
        assert!(check_outline_symbols(&check).is_ok());
    }

    #[test]
    fn outline_symbol_checker_fails_missing_labels() {
        let symbols = driver_document_symbols("postgresql", "select 1;");
        let check = outline_symbol_check(&symbols, &["DROP missing".to_string()]);

        assert!(!check.passed);
        assert!(check_outline_symbols(&check).is_err());
    }

    #[test]
    fn outline_probe_uses_driver_symbols_for_redis() {
        let text = "# ignored\nSET session:1 value\nGET session:1";
        let symbols = driver_document_symbols("redis", text);
        let check = outline_symbol_check(&symbols, &["SET session:1".to_string()]);

        assert_eq!(symbols.len(), 2);
        assert!(check.passed);
        assert_eq!(check.expected[0].line, Some(1));
        assert!(check_outline_symbols(&check).is_ok());
    }

    #[test]
    fn outline_probe_uses_driver_symbols_for_mongodb() {
        let text = "// ignored\ndb.orders.find({ status: \"open\" })";
        let symbols = driver_document_symbols("mongodb", text);
        let check = outline_symbol_check(&symbols, &["find orders".to_string()]);

        assert_eq!(symbols.len(), 1);
        assert!(check.passed);
        assert_eq!(check.expected[0].line, Some(1));
        assert!(check_outline_symbols(&check).is_ok());
    }

    #[test]
    fn generated_large_sql_fixture_scales_statement_count() {
        let fixture = generated_large_sql_fixture(3);

        assert_eq!(fixture.matches("SELECT ").count(), 3);
        assert!(fixture.contains("CREATE TABLE events"));
        assert!(fixture.contains("jsonb_extract_path_text"));
        assert!(fixture.contains("$3"));
    }

    #[test]
    fn viewport_range_for_text_clamps_to_utf8_boundaries() {
        let text = "SELECT 'é'";
        let split_inside = text.find('é').expect("accent") + 1;
        let range = viewport_range_for_text(text, Some(split_inside), Some(1))
            .expect("viewport")
            .expect("range");

        assert_eq!(&text[range], "é");
    }

    #[test]
    fn range_coverage_uses_viewport_local_text() {
        let text = "SELECT 1;\nSELECT 2;";
        let range_start = text.find("SELECT 2").expect("second select");
        let range = range_start..text.len();
        let highlights = vec![Highlight {
            start: range_start,
            end: range_start + "SELECT".len(),
            kind: HighlightKind::Keyword,
        }];
        let coverage = highlight_coverage_for_range(text, range, &highlights);

        assert_eq!(coverage.styled_non_ws_bytes, "SELECT".len());
        assert_eq!(coverage.non_ws_bytes, "SELECT2;".len());
    }

    #[test]
    fn viewport_render_assignment_uses_absolute_ranges() {
        let text = "SELECT 1;\nSELECT \"category_id\"::text;";
        let range_start = text.find("\"category_id\"").expect("identifier");
        let highlights = vec![
            Highlight {
                start: range_start,
                end: range_start + "\"category_id\"".len(),
                kind: HighlightKind::Identifier,
            },
            Highlight {
                start: range_start + "\"category_id\"".len() + 2,
                end: range_start + "\"category_id\"".len() + 6,
                kind: HighlightKind::Type,
            },
        ];
        let rendered_highlights = render_highlight_runs(text, &highlights);
        let check = render_ad_hoc_assignment_check(
            text,
            &rendered_highlights,
            &[AssignmentExpectation {
                token: "\"category_id\"".to_string(),
                kind: HighlightKind::Identifier,
            }],
            &[AssignmentExpectation {
                token: "\"category_id\"".to_string(),
                kind: HighlightKind::String,
            }],
        );

        assert!(check.passed);
    }

    #[test]
    fn measure_highlight_rope_range_reports_repeat_average() {
        let text = generated_large_sql_fixture(3);
        let mut highlighter = SyntaxHighlighter::new().expect("highlighter");
        highlighter.set_language_profile("postgresql");
        let start = text.find("SELECT").expect("select");
        let (highlights, timing) =
            measure_highlight_rope_range(&mut highlighter, &text, start..text.len(), 2, false);

        assert_eq!(timing.repeat, 2);
        assert_eq!(timing.avg_ms, timing.total_ms / 2.0);
        assert!(!highlights.is_empty());
    }
}
