use once_cell::sync::Lazy;
use regex::Regex;

/// Color scheme using ANSI escape codes
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

// SQL Keywords pattern
static KEYWORD_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|OUTER|FULL|ON|AND|OR|NOT|IN|IS|NULL|LIKE|BETWEEN|GROUP|BY|HAVING|ORDER|ASC|DESC|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|ALTER|DROP|TABLE|VIEW|INDEX|AS|DISTINCT|ALL|CASE|WHEN|THEN|ELSE|END|WITH|RECURSIVE|RETURNING|CAST|EXTRACT|INTERVAL|EXISTS|PRIMARY|KEY|FOREIGN|REFERENCES|CONSTRAINT|DEFAULT|UNIQUE|CHECK|CROSS)\b").unwrap()
});

// SQL Functions pattern
static FUNCTION_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(COUNT|SUM|AVG|MIN|MAX|COALESCE|NULLIF|CONCAT|SUBSTRING|LENGTH|UPPER|LOWER|TRIM|ROUND|FLOOR|CEIL|ABS|NOW|CURRENT_DATE|CURRENT_TIME|CURRENT_TIMESTAMP|DATE_TRUNC|EXTRACT)\s*\(").unwrap()
});

// String pattern (single quotes with escape sequences)
static STRING_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"'(?:[^']|''|\\')*'").unwrap());

// Number pattern
static NUMBER_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"\b\d+\.?\d*([eE][+-]?\d+)?\b").unwrap());

// Comment patterns
static LINE_COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"--[^\n]*").unwrap());
static BLOCK_COMMENT_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"/\*[\s\S]*?\*/").unwrap());

// Operator pattern
static OPERATOR_PATTERN: Lazy<Regex> = Lazy::new(|| Regex::new(r"[=<>!]+|[+\-*/%]|\|\||::").unwrap());

/// SQL syntax highlighter using regex patterns.
/// Produces ANSI-escaped strings for headless (non-TUI) display.
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

    /// Highlight SQL text by applying ANSI color codes.
    pub fn highlight_sql(&self, line: &str) -> String {
        if !self.enabled || line.is_empty() {
            return line.to_string();
        }

        let mut highlights: Vec<(usize, usize, &str)> = Vec::new();

        // Comments first (highest priority)
        for mat in LINE_COMMENT_PATTERN.find_iter(line) {
            highlights.push((mat.start(), mat.end(), self.color_scheme.comment));
        }
        for mat in BLOCK_COMMENT_PATTERN.find_iter(line) {
            highlights.push((mat.start(), mat.end(), self.color_scheme.comment));
        }

        // Strings (high priority)
        for mat in STRING_PATTERN.find_iter(line) {
            highlights.push((mat.start(), mat.end(), self.color_scheme.string));
        }

        // Collect other matches
        let mut keyword_matches = Vec::new();
        for mat in KEYWORD_PATTERN.find_iter(line) {
            keyword_matches.push((mat.start(), mat.end()));
        }

        let mut function_matches = Vec::new();
        for mat in FUNCTION_PATTERN.find_iter(line) {
            function_matches.push((mat.start(), mat.end() - 1));
        }

        let mut number_matches = Vec::new();
        for mat in NUMBER_PATTERN.find_iter(line) {
            number_matches.push((mat.start(), mat.end()));
        }

        let mut operator_matches = Vec::new();
        for mat in OPERATOR_PATTERN.find_iter(line) {
            operator_matches.push((mat.start(), mat.end()));
        }

        // Add non-overlapping matches
        let overlaps = |start: usize, end: usize, existing: &[(usize, usize, &str)]| {
            existing.iter().any(|(s, e, _)| {
                (start >= *s && start < *e) || (end > *s && end <= *e) || (start <= *s && end >= *e)
            })
        };

        for (start, end) in keyword_matches {
            if !overlaps(start, end, &highlights) {
                highlights.push((start, end, self.color_scheme.keyword));
            }
        }
        for (start, end) in function_matches {
            if !overlaps(start, end, &highlights) {
                highlights.push((start, end, self.color_scheme.function));
            }
        }
        for (start, end) in number_matches {
            if !overlaps(start, end, &highlights) {
                highlights.push((start, end, self.color_scheme.number));
            }
        }
        for (start, end) in operator_matches {
            if !overlaps(start, end, &highlights) {
                highlights.push((start, end, self.color_scheme.operator));
            }
        }

        highlights.sort_by_key(|h| h.0);

        let mut result = String::with_capacity(line.len() * 2);
        let mut last_pos = 0;
        let reset = self.color_scheme.reset;

        for (start, end, color) in highlights {
            if start < last_pos {
                continue;
            }
            if start > last_pos {
                result.push_str(&line[last_pos..start]);
            }
            result.push_str(color);
            result.push_str(&line[start..end]);
            result.push_str(reset);
            last_pos = end;
        }

        if last_pos < line.len() {
            result.push_str(&line[last_pos..]);
        }

        result
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
}
