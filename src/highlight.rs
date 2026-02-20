use once_cell::sync::Lazy;
use ratatui::style::{Color, Modifier, Style};
use regex::Regex;
use std::ops::Range;

/// Color scheme using ANSI escape codes (for headless/non-TUI output)
pub struct ColorScheme {
    keyword: &'static str,
    function: &'static str,
    string: &'static str,
    number: &'static str,
    comment: &'static str,
    operator: &'static str,
    reset: &'static str,
}

impl ColorScheme {
    fn new() -> Self {
        Self {
            keyword: "\x1b[94m",  // Bright Blue
            function: "\x1b[96m", // Bright Cyan
            string: "\x1b[93m",   // Bright Yellow
            number: "\x1b[95m",   // Bright Magenta
            comment: "\x1b[90m",  // Bright Black (gray)
            operator: "\x1b[0m",  // Default/Reset
            reset: "\x1b[0m",     // Reset All
        }
    }
}

/// Ratatui styles for TUI syntax highlighting — mirrors the ANSI escape codes used in
/// headless mode so the TUI and terminal output look identical.
fn keyword_style() -> Style {
    Style::default().fg(Color::LightBlue) // \x1b[94m Bright Blue
}
fn function_style() -> Style {
    Style::default().fg(Color::LightCyan) // \x1b[96m Bright Cyan
}
fn string_style() -> Style {
    Style::default().fg(Color::LightYellow) // \x1b[93m Bright Yellow
}
fn number_style() -> Style {
    Style::default().fg(Color::LightMagenta) // \x1b[95m Bright Magenta
}
fn comment_style() -> Style {
    Style::default().fg(Color::DarkGray) // \x1b[90m Dark Gray
}

// SQL Keywords pattern
static KEYWORD_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|OUTER|FULL|ON|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|GROUP|BY|HAVING|ORDER|ASC|DESC|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|ALTER|DROP|TABLE|VIEW|INDEX|AS|DISTINCT|ALL|CASE|WHEN|THEN|ELSE|END|WITH|RECURSIVE|RETURNING|CAST|EXTRACT|INTERVAL|EXISTS|PRIMARY|KEY|FOREIGN|REFERENCES|CONSTRAINT|DEFAULT|UNIQUE|CHECK|CROSS)\b").unwrap()
});

// SQL Functions pattern
static FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(COUNT|SUM|AVG|MIN|MAX|COALESCE|NULLIF|CONCAT|SUBSTRING|LENGTH|UPPER|LOWER|TRIM|ROUND|FLOOR|CEIL|ABS|NOW|CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|DATE_TRUNC|EXTRACT)\s*\(").unwrap()
});

// String pattern (single quotes with escape sequences)
static STRING_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"'(?:[^'\\]|''|\\.)*'").unwrap());

// Number pattern
static NUMBER_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b\d+\.?\d*([eE][+-]?\d+)?\b").unwrap());

// Comment patterns (applied to full multi-line text)
static LINE_COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"--[^\n]*").unwrap());
static BLOCK_COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\*[\s\S]*?\*/").unwrap());

// Operator pattern
static OPERATOR_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"[=<>!]+|[+\-*/%]|\|\||::").unwrap());

/// Holds a non-overlapping list of (start, end, priority) spans.
/// Higher priority wins when two spans overlap.
struct SpanList {
    entries: Vec<(usize, usize, u8)>, // (start, end, priority)
}

impl SpanList {
    fn new() -> Self {
        Self { entries: Vec::new() }
    }

    fn add(&mut self, start: usize, end: usize, priority: u8) {
        self.entries.push((start, end, priority));
    }

    /// Returns the highest-priority span that contains `pos`, if any.
    fn style_at(&self, pos: usize) -> Option<u8> {
        self.entries
            .iter()
            .filter(|(s, e, _)| pos >= *s && pos < *e)
            .max_by_key(|(_, _, p)| *p)
            .map(|(_, _, p)| *p)
    }

    /// Collect non-overlapping byte ranges → (start, end, priority), sorted by start.
    /// When multiple spans overlap the same byte, the highest priority wins.
    fn into_sorted_ranges(mut self) -> Vec<(usize, usize, u8)> {
        if self.entries.is_empty() {
            return vec![];
        }
        // Determine overall extent
        let max_end = self.entries.iter().map(|(_, e, _)| *e).max().unwrap_or(0);
        let mut result: Vec<(usize, usize, u8)> = Vec::new();
        let mut i = 0usize;
        while i < max_end {
            let p = self.style_at(i);
            match p {
                None => i += 1,
                Some(prio) => {
                    // Find how far this priority spans continuously
                    let start = i;
                    while i < max_end && self.style_at(i) == Some(prio) {
                        i += 1;
                    }
                    result.push((start, i, prio));
                }
            }
        }
        result
    }
}

// Map priority numbers to tokens
const PRIO_COMMENT: u8 = 5;
const PRIO_STRING: u8 = 4;
const PRIO_FUNCTION: u8 = 3;
const PRIO_KEYWORD: u8 = 2;
const PRIO_NUMBER: u8 = 1;

fn prio_to_ratatui_style(prio: u8) -> Style {
    match prio {
        PRIO_COMMENT => comment_style(),
        PRIO_STRING => string_style(),
        PRIO_FUNCTION => function_style(),
        PRIO_KEYWORD => keyword_style(),
        PRIO_NUMBER => number_style(),
        _ => Style::default(),
    }
}

fn prio_to_ansi_color<'a>(prio: u8, scheme: &'a ColorScheme) -> &'a str {
    match prio {
        PRIO_COMMENT => scheme.comment,
        PRIO_STRING => scheme.string,
        PRIO_FUNCTION => scheme.function,
        PRIO_KEYWORD => scheme.keyword,
        PRIO_NUMBER => scheme.number,
        _ => scheme.reset,
    }
}

/// Compute raw highlight ranges for `text` (multi-line OK).
/// Returns a sorted list of non-overlapping `(byte_range, priority)` pairs.
fn compute_ranges(text: &str) -> Vec<(Range<usize>, u8)> {
    let mut spans = SpanList::new();

    for mat in LINE_COMMENT_PATTERN.find_iter(text) {
        spans.add(mat.start(), mat.end(), PRIO_COMMENT);
    }
    for mat in BLOCK_COMMENT_PATTERN.find_iter(text) {
        spans.add(mat.start(), mat.end(), PRIO_COMMENT);
    }
    for mat in STRING_PATTERN.find_iter(text) {
        spans.add(mat.start(), mat.end(), PRIO_STRING);
    }
    for mat in FUNCTION_PATTERN.find_iter(text) {
        // Don't include the opening paren in the function name span
        spans.add(mat.start(), mat.end() - 1, PRIO_FUNCTION);
    }
    for mat in KEYWORD_PATTERN.find_iter(text) {
        spans.add(mat.start(), mat.end(), PRIO_KEYWORD);
    }
    for mat in NUMBER_PATTERN.find_iter(text) {
        spans.add(mat.start(), mat.end(), PRIO_NUMBER);
    }

    spans
        .into_sorted_ranges()
        .into_iter()
        .map(|(s, e, p)| (s..e, p))
        .collect()
}

/// SQL syntax highlighter.
/// Produces ANSI-escaped strings for headless display and ratatui `Style` spans for TUI.
pub struct SqlHighlighter {
    color_scheme: ColorScheme,
    enabled: bool,
}

impl SqlHighlighter {
    pub fn new(enabled: bool) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Self {
            color_scheme: ColorScheme::new(),
            enabled,
        })
    }

    /// Highlight SQL text by applying ANSI color codes (headless / non-TUI mode).
    pub fn highlight_sql(&self, line: &str) -> String {
        if !self.enabled || line.is_empty() {
            return line.to_string();
        }

        let ranges = compute_ranges(line);
        let scheme = &self.color_scheme;
        let mut result = String::with_capacity(line.len() * 2);
        let mut last = 0usize;

        for (range, prio) in &ranges {
            if range.start > last {
                result.push_str(&line[last..range.start]);
            }
            result.push_str(prio_to_ansi_color(*prio, scheme));
            result.push_str(&line[range.start..range.end]);
            result.push_str(scheme.reset);
            last = range.end;
        }
        if last < line.len() {
            result.push_str(&line[last..]);
        }
        result
    }

    /// Return byte-range → ratatui `Style` spans for the given (possibly multi-line) SQL text.
    /// Spans are non-overlapping and sorted by start offset.
    pub fn highlight_to_spans(&self, text: &str) -> Vec<(Range<usize>, Style)> {
        if !self.enabled || text.is_empty() {
            return vec![];
        }
        compute_ranges(text)
            .into_iter()
            .map(|(range, prio)| (range, prio_to_ratatui_style(prio)))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_disabled_highlighter() {
        let highlighter = SqlHighlighter::new(false).unwrap();
        let result = highlighter.highlight_sql("SELECT * FROM users");
        assert_eq!(result, "SELECT * FROM users");
    }

    #[test]
    fn test_keyword_highlighting() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT");
        assert!(result.contains("\x1b[94m"));
        assert!(result.contains("SELECT"));
        assert!(result.contains("\x1b[0m"));
    }

    #[test]
    fn test_string_highlighting() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT 'hello'");
        assert!(result.contains("\x1b[93m"));
    }

    #[test]
    fn test_number_highlighting() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT 42");
        assert!(result.contains("\x1b[95m"));
    }

    #[test]
    fn test_comment_highlighting() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("-- comment");
        assert!(result.contains("\x1b[90m"));
    }

    #[test]
    fn test_function_highlighting() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT COUNT(*)");
        assert!(result.contains("\x1b[96m"));
    }

    #[test]
    fn test_complex_query() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let query = "SELECT id, name FROM users WHERE status = 'active'";
        let result = highlighter.highlight_sql(query);
        assert!(result.contains("\x1b[94m"));
        assert!(result.contains("\x1b[93m"));
        assert!(result.contains("\x1b[0m"));
    }

    #[test]
    fn test_keywords_in_strings_not_highlighted() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT 'SELECT FROM WHERE'");
        // Only the outer SELECT keyword should be highlighted; the ones inside the string should not
        let keyword_count = result.matches("\x1b[94m").count();
        assert_eq!(keyword_count, 1);
    }

    #[test]
    fn test_malformed_sql_graceful() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT FROM WHERE");
        assert!(result.contains("\x1b[94m"));
    }

    #[test]
    fn test_empty_string() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("");
        assert_eq!(result, "");
    }

    #[test]
    fn test_multiline_fragment() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("FROM users");
        assert!(result.contains("\x1b[94m"));
    }

    #[test]
    fn test_operators() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("WHERE x = 1 AND y > 2");
        assert!(result.contains("\x1b[94m"));
        assert!(result.contains("="));
        assert!(result.contains(">"));
    }

    #[test]
    fn test_block_comment() {
        let highlighter = SqlHighlighter::new(true).unwrap();
        let result = highlighter.highlight_sql("SELECT /* comment */ 1");
        assert!(result.contains("\x1b[90m"));
    }

    #[test]
    fn test_highlight_to_spans_disabled() {
        let h = SqlHighlighter::new(false).unwrap();
        let spans = h.highlight_to_spans("SELECT 1");
        assert!(spans.is_empty());
    }

    #[test]
    fn test_highlight_to_spans_keywords() {
        let h = SqlHighlighter::new(true).unwrap();
        let spans = h.highlight_to_spans("SELECT 1 FROM t");
        // Should have spans for SELECT and FROM
        assert!(!spans.is_empty());
        // SELECT is at offset 0..6
        let select_span = spans.iter().find(|(r, _)| r.start == 0 && r.end == 6);
        assert!(select_span.is_some());
    }

    #[test]
    fn test_highlight_to_spans_multiline() {
        let h = SqlHighlighter::new(true).unwrap();
        let sql = "SELECT id\nFROM orders";
        let spans = h.highlight_to_spans(sql);
        assert!(!spans.is_empty());
        // Should have spans for both SELECT and FROM
        let has_select = spans.iter().any(|(r, _)| r.start == 0 && r.end == 6);
        let from_offset = sql.find("FROM").unwrap();
        let has_from = spans.iter().any(|(r, _)| r.start == from_offset);
        assert!(has_select);
        assert!(has_from);
    }

    #[test]
    fn test_highlight_to_spans_no_overlap() {
        let h = SqlHighlighter::new(true).unwrap();
        let sql = "SELECT id, name FROM orders WHERE id = 1 AND name = 'Alice'";
        let spans = h.highlight_to_spans(sql);
        // Ensure spans are non-overlapping and sorted
        for i in 1..spans.len() {
            assert!(
                spans[i].0.start >= spans[i - 1].0.end,
                "Spans overlap: {:?} and {:?}",
                spans[i - 1].0,
                spans[i].0
            );
        }
    }
}
