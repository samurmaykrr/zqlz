//! `money` display that matches psql.
//!
//! The binary form is a bare integer (scaled by the server's `lc_monetary`
//! `frac_digits`), so the text has to be rebuilt the way `cash_out` does it.
//! Rather than re-implementing the C library's `lconv` handling, the layout is
//! learned from the server's own text output (`MoneyFormat::parse_probe`).

use std::cell::RefCell;
use std::sync::{Arc, Mutex};

/// Query whose three text results describe the session's monetary layout.
pub(crate) const MONEY_PROBE_SQL: &str = "SELECT (1234567890123.45::money)::text, \
     (-1234567890123.45::money)::text, (0.05::money)::text";

/// Per-connection cache of the probed layout; cleared when the session's
/// `lc_monetary` may have changed.
pub(crate) type MoneyFormatCache = Arc<Mutex<Option<MoneyFormat>>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MoneyFormat {
    positive_prefix: String,
    positive_suffix: String,
    negative_prefix: String,
    negative_suffix: String,
    thousands_sep: String,
    decimal_sep: String,
    frac_digits: usize,
    /// Digits per group; `cash_out` only honours the first `mon_grouping` entry.
    group_size: usize,
}

impl Default for MoneyFormat {
    /// The C-locale form, also the fallback when the probe fails.
    fn default() -> Self {
        Self {
            positive_prefix: "$".into(),
            positive_suffix: String::new(),
            negative_prefix: "-$".into(),
            negative_suffix: String::new(),
            thousands_sep: ",".into(),
            decimal_sep: ".".into(),
            frac_digits: 2,
            group_size: 3,
        }
    }
}

/// Splits server money text into (text before the first digit, digits and
/// separators, text after the last digit).
fn split_digits(text: &str) -> Option<(&str, &str, &str)> {
    let start = text.find(|c: char| c.is_ascii_digit())?;
    let end = text.rfind(|c: char| c.is_ascii_digit())? + 1;
    Some((&text[..start], &text[start..end], &text[end..]))
}

impl MoneyFormat {
    /// Builds a layout from the probe's `(1234567890123.45, -1234567890123.45, 0.05)`
    /// outputs, or `None` when they do not look like grouped money amounts.
    pub(crate) fn parse_probe(large: &str, negative_large: &str, small: &str) -> Option<Self> {
        let (positive_prefix, big, positive_suffix) = split_digits(large)?;
        let (negative_prefix, _, negative_suffix) = split_digits(negative_large)?;
        let (_, small_inner, _) = split_digits(small)?;

        // "0.05" keeps its decimal separator only when the locale has fractional digits.
        let (decimal_sep, frac_digits) = match small_inner.rfind(|c: char| !c.is_ascii_digit()) {
            Some(at) => {
                let separator = small_inner[at..].chars().next()?;
                let fraction = &small_inner[at + separator.len_utf8()..];
                (separator.to_string(), fraction.len())
            }
            None => (".".to_string(), 0),
        };

        let integral = if frac_digits > 0 {
            big.get(..big.len().checked_sub(frac_digits + decimal_sep.len())?)?
        } else {
            big
        };
        let last_group = integral.chars().rev().take_while(char::is_ascii_digit).count();
        let head = &integral[..integral.len() - last_group];
        let thousands_sep = &head[head.trim_end_matches(|c: char| !c.is_ascii_digit()).len()..];
        if last_group == 0 || thousands_sep.is_empty() {
            return None;
        }

        Some(Self {
            positive_prefix: positive_prefix.into(),
            positive_suffix: positive_suffix.into(),
            negative_prefix: negative_prefix.into(),
            negative_suffix: negative_suffix.into(),
            thousands_sep: thousands_sep.into(),
            decimal_sep,
            frac_digits,
            group_size: last_group,
        })
    }

    /// Renders the server's raw `money` integer like `cash_out`.
    pub(crate) fn format(&self, raw: i64) -> String {
        let digits = raw.unsigned_abs().to_string();
        let digits = format!("{digits:0>width$}", width = self.frac_digits + 1);
        let (integral, fraction) = digits.split_at(digits.len() - self.frac_digits);

        let mut grouped = String::new();
        for (index, digit) in integral.chars().enumerate() {
            if index > 0 && (integral.len() - index) % self.group_size == 0 {
                grouped.push_str(&self.thousands_sep);
            }
            grouped.push(digit);
        }
        if self.frac_digits > 0 {
            grouped.push_str(&self.decimal_sep);
            grouped.push_str(fraction);
        }

        let (prefix, suffix) = if raw < 0 {
            (&self.negative_prefix, &self.negative_suffix)
        } else {
            (&self.positive_prefix, &self.positive_suffix)
        };
        format!("{prefix}{grouped}{suffix}")
    }
}

thread_local! {
    static ACTIVE: RefCell<MoneyFormat> = RefCell::new(MoneyFormat::default());
}

/// Runs a synchronous decode with `format` as the layout every `money` value
/// (including ones nested in arrays and composites, which `FromSql` reaches
/// without any connection context) is rendered with.
pub(crate) fn with_money_format<T>(format: &MoneyFormat, decode: impl FnOnce() -> T) -> T {
    let previous = ACTIVE.with(|active| std::mem::replace(&mut *active.borrow_mut(), format.clone()));
    let result = decode();
    ACTIVE.with(|active| *active.borrow_mut() = previous);
    result
}

pub(crate) fn format_active_money(raw: i64) -> String {
    ACTIVE.with(|active| active.borrow().format(raw))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn probe(large: &str, negative: &str, small: &str) -> MoneyFormat {
        MoneyFormat::parse_probe(large, negative, small).unwrap()
    }

    #[test]
    fn en_us_formats_like_psql() {
        let format = probe("$1,234,567,890,123.45", "-$1,234,567,890,123.45", "$0.05");
        assert_eq!(format.format(123456), "$1,234.56");
        assert_eq!(format.format(-123456789), "-$1,234,567.89");
        assert_eq!(format.format(0), "$0.00");
        assert_eq!(format.format(5), "$0.05");
        assert_eq!(format.format(-5), "-$0.05");
        assert_eq!(format.format(100000), "$1,000.00");
        assert_eq!(format.format(i64::MIN), "-$92,233,720,368,547,758.08");
        assert_eq!(format, MoneyFormat::default());
    }

    #[test]
    fn de_de_formats_like_psql() {
        let format = probe(
            "1.234.567.890.123,45 €",
            "-1.234.567.890.123,45 €",
            "0,05 €",
        );
        assert_eq!(format.format(123456), "1.234,56 €");
        assert_eq!(format.format(-123456), "-1.234,56 €");
        assert_eq!(format.format(5), "0,05 €");
    }

    #[test]
    fn ja_jp_has_no_fraction() {
        let format = probe("￥1,234,567,890,123", "-￥1,234,567,890,123", "￥0");
        assert_eq!(format.format(1235), "￥1,235");
        assert_eq!(format.format(-1235), "-￥1,235");
        assert_eq!(format.format(0), "￥0");
    }

    #[test]
    fn parenthesised_negatives_and_odd_grouping() {
        let format = probe("$1,234,567,890,123.45", "($1,234,567,890,123.45)", "$0.05");
        assert_eq!(format.format(-123456), "($1,234.56)");

        let format = probe("1 234 567 890 123,45 kr", "-1 234 567 890 123,45 kr", "0,05 kr");
        assert_eq!(format.format(123456789), "1 234 567,89 kr");
    }

    #[test]
    fn unparsable_probe_is_rejected() {
        assert!(MoneyFormat::parse_probe("n/a", "n/a", "n/a").is_none());
    }

    #[test]
    fn active_format_is_scoped() {
        let german = probe("1.234.567.890.123,45 €", "-1.234.567.890.123,45 €", "0,05 €");
        assert_eq!(with_money_format(&german, || format_active_money(123456)), "1.234,56 €");
        assert_eq!(format_active_money(123456), "$1,234.56");
    }
}
