//! How the table viewer fetches rows.
//!
//! Opening, paging, refreshing, jumping to the last page and loading more all go
//! through [`plan_fetch`], so the window, the ORDER BY and the counting strategy
//! are decided in exactly one place.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FetchMode {
    /// Show the given 1-based page.
    Page(usize),
    /// Re-read the current page and recount.
    Refresh,
    /// Jump to the final page.
    Last,
    /// Append the next batch after the rows already loaded.
    LoadMore,
}

/// Everything about the viewer that shapes a fetch, captured at request time.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct QuerySnapshot {
    /// Filters and search, rendered as SQL predicates.
    pub where_clauses: Vec<String>,
    /// User sorts, rendered as `<quoted column> ASC|DESC`.
    pub order_by_clauses: Vec<String>,
    /// Columns to select; empty selects every column.
    pub visible_columns: Vec<String>,
    /// Primary-key columns, already quoted for the connection.
    pub pk_columns: Vec<String>,
    /// Rows per page, or `None` when the limit is switched off.
    pub page_size: Option<usize>,
    pub current_page: usize,
    /// Rows already in the grid; where load-more continues from.
    pub loaded_rows: usize,
    pub known_total: Option<KnownTotal>,
    pub fast_exact_count: bool,
    /// A page load is in flight, so appending to the current rows is pointless.
    pub is_loading: bool,
}

#[derive(Clone, Debug, PartialEq)]
pub struct KnownTotal {
    pub rows: u64,
    pub is_estimated: bool,
    /// The filters this total was counted under.
    pub where_clauses: Vec<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RowWindow {
    /// Every matching row.
    All,
    Forward {
        limit: usize,
        offset: usize,
    },
    /// Rows read backwards from the end: the plan's ORDER BY is already reversed,
    /// so the rows arrive last-first and must be flipped.
    FromEnd {
        limit: usize,
        skip: usize,
    },
    /// The final page, whose window depends on an exact count. Count first, then
    /// plan [`FetchMode::Last`] again with that total.
    LastPage,
}

#[derive(Clone, Debug, PartialEq)]
pub enum CountPlan {
    Reuse(KnownTotal),
    Exact,
    Estimate,
    /// Leave the total unknown; whether there is more comes from the row count.
    Skip,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchPlan {
    pub window: RowWindow,
    /// The ORDER BY to emit, already in the window's direction.
    pub order_by: Vec<String>,
    pub count: CountPlan,
    pub page: usize,
    /// Append to the loaded rows instead of replacing them.
    pub append: bool,
}

/// Decide how to fetch rows for `mode`. `None` means there is nothing to fetch.
pub fn plan_fetch(mode: FetchMode, query: &QuerySnapshot) -> Option<FetchPlan> {
    let order_by = stable_order_by(query);
    let reusable_total = query
        .known_total
        .as_ref()
        .filter(|total| mode != FetchMode::Refresh && total.where_clauses == query.where_clauses)
        .cloned();
    let exact_total = reusable_total
        .as_ref()
        .filter(|total| !total.is_estimated)
        .map(|total| total.rows);

    let Some(page_size) = query.page_size.filter(|page_size| *page_size > 0) else {
        return (mode != FetchMode::LoadMore).then_some(FetchPlan {
            window: RowWindow::All,
            order_by,
            count: CountPlan::Skip,
            page: 1,
            append: false,
        });
    };

    let page = match mode {
        FetchMode::LoadMore => {
            return (!query.is_loading).then_some(FetchPlan {
                window: RowWindow::Forward {
                    limit: page_size,
                    offset: query.loaded_rows,
                },
                order_by,
                count: CountPlan::Skip,
                page: query.current_page.max(1),
                append: true,
            });
        }
        FetchMode::Page(page) => page.max(1),
        FetchMode::Refresh => query.current_page.max(1),
        FetchMode::Last => match exact_total {
            Some(total) => last_page(total, page_size),
            None => {
                return Some(FetchPlan {
                    window: RowWindow::LastPage,
                    order_by,
                    count: CountPlan::Exact,
                    page: query.current_page.max(1),
                    append: false,
                });
            }
        },
    };

    let offset = (page - 1).saturating_mul(page_size);
    let count = match reusable_total {
        Some(total) => CountPlan::Reuse(total),
        None if query.fast_exact_count => CountPlan::Exact,
        // An estimate describes the whole table, so it is wrong under any filter.
        None if query.where_clauses.is_empty() => CountPlan::Estimate,
        None => CountPlan::Skip,
    };

    // Reading from the end turns a deep OFFSET into a shallow one. That is only
    // correct when the total is exact and a primary key makes the order total.
    let remaining_after_offset = exact_total
        .filter(|total| !query.pk_columns.is_empty() && offset as u64 > total / 2)
        .and_then(|total| total.checked_sub(offset as u64))
        .filter(|remaining| *remaining > 0)
        .map(|remaining| usize::try_from(remaining).unwrap_or(usize::MAX));

    let (window, order_by) = match remaining_after_offset {
        Some(remaining) => (
            RowWindow::FromEnd {
                limit: page_size.min(remaining),
                skip: remaining.saturating_sub(page_size),
            },
            order_by
                .iter()
                .map(|clause| reverse_order_by_clause(clause))
                .collect(),
        ),
        None => (
            RowWindow::Forward {
                limit: page_size,
                offset,
            },
            order_by,
        ),
    };

    Some(FetchPlan {
        window,
        order_by,
        count,
        page,
        append: false,
    })
}

pub fn last_page(total_rows: u64, page_size: usize) -> usize {
    let pages = total_rows.div_ceil(page_size.max(1) as u64).max(1);
    usize::try_from(pages).unwrap_or(usize::MAX)
}

/// The user's sorts with the primary key appended, so every page sees the same
/// row order and forward and reversed reads agree.
fn stable_order_by(query: &QuerySnapshot) -> Vec<String> {
    let mut order_by = query.order_by_clauses.clone();
    for pk_column in &query.pk_columns {
        // Some engines (SQL Server) reject a column listed twice in ORDER BY.
        if !order_by
            .iter()
            .any(|clause| parse_order_term(clause).expression == pk_column)
        {
            order_by.push(format!("{pk_column} ASC"));
        }
    }
    order_by
}

struct OrderTerm<'a> {
    expression: &'a str,
    descending: bool,
    nulls_first: Option<bool>,
}

fn strip_trailing_keyword<'a>(text: &'a str, keyword: &str) -> Option<&'a str> {
    let (head, tail) = text.trim_end().rsplit_once(char::is_whitespace)?;
    tail.eq_ignore_ascii_case(keyword).then(|| head.trim_end())
}

fn parse_order_term(clause: &str) -> OrderTerm<'_> {
    let mut expression = clause.trim();
    let mut nulls_first = None;
    for (keyword, first) in [("FIRST", true), ("LAST", false)] {
        if let Some(head) = strip_trailing_keyword(expression, keyword)
            .and_then(|head| strip_trailing_keyword(head, "NULLS"))
        {
            expression = head;
            nulls_first = Some(first);
            break;
        }
    }

    let mut descending = false;
    if let Some(head) = strip_trailing_keyword(expression, "DESC") {
        expression = head;
        descending = true;
    } else if let Some(head) = strip_trailing_keyword(expression, "ASC") {
        expression = head;
    }

    OrderTerm {
        expression,
        descending,
        nulls_first,
    }
}

/// Flip an ORDER BY term, including where NULLs land, so the reversed read
/// returns exactly the forward order backwards.
pub fn reverse_order_by_clause(clause: &str) -> String {
    let term = parse_order_term(clause);
    let direction = if term.descending { "ASC" } else { "DESC" };
    match term.nulls_first {
        Some(true) => format!("{} {direction} NULLS LAST", term.expression),
        Some(false) => format!("{} {direction} NULLS FIRST", term.expression),
        None => format!("{} {direction}", term.expression),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snapshot() -> QuerySnapshot {
        QuerySnapshot {
            pk_columns: vec!["\"id\"".to_string()],
            page_size: Some(100),
            current_page: 1,
            ..QuerySnapshot::default()
        }
    }

    fn total(rows: u64, is_estimated: bool, where_clauses: &[&str]) -> Option<KnownTotal> {
        Some(KnownTotal {
            rows,
            is_estimated,
            where_clauses: where_clauses
                .iter()
                .map(|clause| clause.to_string())
                .collect(),
        })
    }

    fn plan(mode: FetchMode, query: &QuerySnapshot) -> FetchPlan {
        plan_fetch(mode, query).expect("a plan")
    }

    #[test]
    fn filtered_count_never_estimates() {
        let query = QuerySnapshot {
            where_clauses: vec!["status = 'open'".to_string()],
            ..snapshot()
        };
        assert_eq!(plan(FetchMode::Page(1), &query).count, CountPlan::Skip);

        let unfiltered = snapshot();
        assert_eq!(
            plan(FetchMode::Page(1), &unfiltered).count,
            CountPlan::Estimate
        );
    }

    #[test]
    fn total_reused_only_for_same_filters() {
        let counted_unfiltered = QuerySnapshot {
            where_clauses: vec!["status = 'open'".to_string()],
            known_total: total(5000, false, &[]),
            ..snapshot()
        };
        assert_eq!(
            plan(FetchMode::Page(2), &counted_unfiltered).count,
            CountPlan::Skip
        );

        let counted_same = QuerySnapshot {
            known_total: total(5000, false, &["status = 'open'"]),
            ..counted_unfiltered
        };
        assert!(matches!(
            plan(FetchMode::Page(2), &counted_same).count,
            CountPlan::Reuse(KnownTotal { rows: 5000, .. })
        ));
    }

    #[test]
    fn refresh_keeps_page_and_offset() {
        let query = QuerySnapshot {
            current_page: 5,
            known_total: total(10_000, true, &[]),
            ..snapshot()
        };
        let plan = plan(FetchMode::Refresh, &query);
        assert_eq!(plan.page, 5);
        assert_eq!(
            plan.window,
            RowWindow::Forward {
                limit: 100,
                offset: 400
            }
        );
        assert_eq!(plan.count, CountPlan::Estimate, "refresh recounts");
    }

    #[test]
    fn filters_change_resets_to_page_one() {
        let query = QuerySnapshot {
            current_page: 7,
            where_clauses: vec!["status = 'open'".to_string()],
            known_total: total(10_000, false, &[]),
            ..snapshot()
        };
        let plan = plan(FetchMode::Page(1), &query);
        assert_eq!(plan.page, 1);
        assert_eq!(
            plan.window,
            RowWindow::Forward {
                limit: 100,
                offset: 0
            }
        );
        assert_eq!(plan.count, CountPlan::Skip);
    }

    #[test]
    fn limit_off_has_no_limit() {
        let query = QuerySnapshot {
            page_size: None,
            current_page: 3,
            ..snapshot()
        };
        let plan = plan(FetchMode::Refresh, &query);
        assert_eq!(plan.window, RowWindow::All);
        assert_eq!(plan.page, 1);
        assert_eq!(plan_fetch(FetchMode::LoadMore, &query), None);
    }

    #[test]
    fn open_page_size_matches_next_page_offset() {
        let query = QuerySnapshot {
            page_size: Some(250),
            ..snapshot()
        };
        let RowWindow::Forward { limit, .. } = plan(FetchMode::Page(1), &query).window else {
            panic!("page one reads forward");
        };
        let RowWindow::Forward { offset, .. } = plan(FetchMode::Page(2), &query).window else {
            panic!("page two reads forward without an exact total");
        };
        assert_eq!(limit, offset, "page two starts where page one ended");
    }

    #[test]
    fn reversed_requires_exact_total_and_pk() {
        let exact = QuerySnapshot {
            known_total: total(1000, false, &[]),
            ..snapshot()
        };
        assert_eq!(
            plan(FetchMode::Page(9), &exact).window,
            RowWindow::FromEnd {
                limit: 100,
                skip: 100
            }
        );
        assert_eq!(
            plan(FetchMode::Page(9), &exact).order_by,
            vec!["\"id\" DESC"]
        );

        let estimated = QuerySnapshot {
            known_total: total(1000, true, &[]),
            ..snapshot()
        };
        assert!(matches!(
            plan(FetchMode::Page(9), &estimated).window,
            RowWindow::Forward { offset: 800, .. }
        ));

        let without_pk = QuerySnapshot {
            pk_columns: Vec::new(),
            ..exact
        };
        assert!(matches!(
            plan(FetchMode::Page(9), &without_pk).window,
            RowWindow::Forward { offset: 800, .. }
        ));
    }

    #[test]
    fn last_page_fetches_remainder() {
        let unknown = snapshot();
        let counting = plan(FetchMode::Last, &unknown);
        assert_eq!(counting.window, RowWindow::LastPage);
        assert_eq!(counting.count, CountPlan::Exact);

        let counted = QuerySnapshot {
            known_total: total(1050, false, &[]),
            ..snapshot()
        };
        let last = plan(FetchMode::Last, &counted);
        assert_eq!(last.page, 11);
        assert_eq!(last.window, RowWindow::FromEnd { limit: 50, skip: 0 });

        let without_pk = QuerySnapshot {
            pk_columns: Vec::new(),
            ..counted
        };
        assert_eq!(
            plan(FetchMode::Last, &without_pk).window,
            RowWindow::Forward {
                limit: 100,
                offset: 1000
            }
        );
    }

    #[test]
    fn load_more_skipped_while_loading() {
        let query = QuerySnapshot {
            loaded_rows: 300,
            ..snapshot()
        };
        let more = plan(FetchMode::LoadMore, &query);
        assert!(more.append);
        assert_eq!(
            more.window,
            RowWindow::Forward {
                limit: 100,
                offset: 300
            }
        );

        let loading = QuerySnapshot {
            is_loading: true,
            ..query
        };
        assert_eq!(plan_fetch(FetchMode::LoadMore, &loading), None);
    }

    #[test]
    fn order_by_always_has_pk_tiebreaker() {
        let sorted = QuerySnapshot {
            order_by_clauses: vec!["\"name\" DESC".to_string()],
            ..snapshot()
        };
        assert_eq!(
            plan(FetchMode::Page(1), &sorted).order_by,
            vec!["\"name\" DESC", "\"id\" ASC"]
        );

        let sorted_by_pk = QuerySnapshot {
            order_by_clauses: vec!["\"id\" DESC".to_string()],
            ..snapshot()
        };
        assert_eq!(
            plan(FetchMode::Page(1), &sorted_by_pk).order_by,
            vec!["\"id\" DESC"]
        );
    }

    #[test]
    fn reversing_flips_direction_and_null_placement() {
        assert_eq!(reverse_order_by_clause("\"a\" ASC"), "\"a\" DESC");
        assert_eq!(reverse_order_by_clause("\"a\""), "\"a\" DESC");
        assert_eq!(reverse_order_by_clause("`a` desc"), "`a` ASC");
        assert_eq!(
            reverse_order_by_clause("\"a\" ASC NULLS FIRST"),
            "\"a\" DESC NULLS LAST"
        );
        assert_eq!(
            reverse_order_by_clause("\"Mixed Case\" DESC nulls last"),
            "\"Mixed Case\" ASC NULLS FIRST"
        );
    }
}
