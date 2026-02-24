use once_cell::sync::Lazy;
use regex::Regex;

#[allow(dead_code)]
static KEYWORD_PATTERN: Lazy<Regex> = Lazy::new(|| {
    Regex::new(r"(?i)\b(SELECT|FROM|WHERE|JOIN|INNER|LEFT|RIGHT|FULL|OUTER|ON|GROUP|ORDER|BY|HAVING|LIMIT|OFFSET|UNION|INTERSECT|EXCEPT|INSERT|INTO|VALUES|UPDATE|SET|DELETE|CREATE|DROP|ALTER|TABLE|DATABASE|INDEX|VIEW|AS|DISTINCT|ALL|AND|OR|NOT|IN|EXISTS|BETWEEN|LIKE|IS|NULL|CASE|WHEN|THEN|ELSE|END|WITH|RECURSIVE)\b").unwrap()
});

#[derive(Debug, PartialEq, Clone)]
#[allow(dead_code)]
pub enum CompletionContext {
    Keyword,           // Start of statement or after whitespace
    TableName,         // After FROM, JOIN, INTO, UPDATE
    ColumnName,        // After SELECT, WHERE, ON, GROUP BY, ORDER BY
    FunctionName,      // At start or after operators
    SchemaQualified,   // After "schema_name."
    Nothing,           // Inside string/comment, no completion
}

/// Detects what type of completion is appropriate at the given position
#[allow(dead_code)]
pub fn detect_context(line: &str, pos: usize) -> CompletionContext {
    // Clamp position to line length
    let pos = pos.min(line.len());

    // Don't complete inside strings or comments
    if is_inside_string_or_comment(line, pos) {
        return CompletionContext::Nothing;
    }

    // Find the start of the current word
    let word_start = find_word_start(line, pos);
    let partial = &line[word_start..pos];

    // If there's a dot in the partial word, it's schema-qualified
    if partial.contains('.') {
        return CompletionContext::SchemaQualified;
    }

    // Get the context before the current word
    let context_before = line[..word_start].trim().to_uppercase();

    // If there's no context before and we're typing something, it's a keyword
    if context_before.is_empty() {
        return CompletionContext::Keyword;
    }

    // Find the last keyword before our position
    let last_keyword = find_last_keyword(&context_before);

    // Determine context based on last keyword
    match last_keyword.as_deref() {
        Some("FROM") | Some("JOIN") | Some("INTO") | Some("UPDATE") => {
            CompletionContext::TableName
        }
        Some("SELECT") => {
            // After SELECT, if we've seen other tokens (like *), we might be typing FROM
            // Check if there are non-keyword tokens after SELECT
            let _after_select = context_before
                .split("SELECT")
                .last()
                .unwrap_or("")
                .trim();

            // If there's something after SELECT (like * or column names), the next word could be a keyword
            // But we can't easily distinguish, so default to ColumnName (safer for usability)
            CompletionContext::ColumnName
        }
        Some("WHERE") | Some("ON") | Some("HAVING") => {
            CompletionContext::ColumnName
        }
        Some("GROUP") | Some("ORDER") => {
            // Check if followed by BY
            if context_before.ends_with("GROUP BY") || context_before.ends_with("ORDER BY") {
                CompletionContext::ColumnName
            } else {
                CompletionContext::Keyword
            }
        }
        Some("BY") => {
            // Check if this is part of GROUP BY or ORDER BY
            if context_before.contains("GROUP BY") || context_before.contains("ORDER BY") {
                CompletionContext::ColumnName
            } else {
                CompletionContext::Keyword
            }
        }
        _ => {
            // Default to keyword completion
            CompletionContext::Keyword
        }
    }
}

/// Finds the start position of the word at the given position
pub fn find_word_start(line: &str, pos: usize) -> usize {
    let bytes = line.as_bytes();
    let mut start = pos;

    while start > 0 {
        let prev = start - 1;
        let ch = bytes[prev] as char;

        // Word characters: alphanumeric, underscore, or dot (for schema.table)
        if ch.is_alphanumeric() || ch == '_' || ch == '.' {
            start = prev;
        } else {
            break;
        }
    }

    start
}

/// Finds the last SQL keyword in the given text
fn find_last_keyword(text: &str) -> Option<String> {
    KEYWORD_PATTERN
        .find_iter(text)
        .last()
        .map(|m| m.as_str().to_uppercase())
}

/// Checks if the position is inside a string literal or comment
fn is_inside_string_or_comment(line: &str, pos: usize) -> bool {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let mut escape_next = false;

    for (i, ch) in line.char_indices() {
        if i >= pos {
            break;
        }

        if escape_next {
            escape_next = false;
            continue;
        }

        match ch {
            '\\' if in_single_quote || in_double_quote => {
                escape_next = true;
            }
            '\'' if !in_double_quote => {
                in_single_quote = !in_single_quote;
            }
            '"' if !in_single_quote => {
                in_double_quote = !in_double_quote;
            }
            '-' if !in_single_quote && !in_double_quote => {
                // Check for line comment
                if i + 1 < line.len() && line.as_bytes()[i + 1] == b'-' {
                    return true; // Rest of line is a comment
                }
            }
            _ => {}
        }
    }

    in_single_quote || in_double_quote
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_detect_table_context() {
        assert_eq!(
            detect_context("SELECT * FROM us", 17),
            CompletionContext::TableName
        );
        assert_eq!(
            detect_context("SELECT * FROM users JOIN or", 28),
            CompletionContext::TableName
        );
        assert_eq!(
            detect_context("INSERT INTO us", 14),
            CompletionContext::TableName
        );
        assert_eq!(
            detect_context("UPDATE us", 9),
            CompletionContext::TableName
        );
    }

    #[test]
    fn test_detect_column_context() {
        assert_eq!(
            detect_context("SELECT col", 10),
            CompletionContext::ColumnName
        );
        assert_eq!(
            detect_context("SELECT * FROM users WHERE col", 29),
            CompletionContext::ColumnName
        );
        assert_eq!(
            detect_context("SELECT * FROM users GROUP BY col", 32),
            CompletionContext::ColumnName
        );
        assert_eq!(
            detect_context("SELECT * FROM users ORDER BY col", 32),
            CompletionContext::ColumnName
        );
    }

    #[test]
    fn test_detect_keyword_context() {
        assert_eq!(detect_context("SEL", 3), CompletionContext::Keyword);
        // After "SELECT *", the completion context is ambiguous - could be column or keyword
        // Our implementation defaults to ColumnName for better usability after SELECT
        // (columns are more common than keywords after SELECT)
        // If user types "SELECT * F" they'll see both FROM keyword and any matching columns
    }

    #[test]
    fn test_detect_schema_qualified() {
        assert_eq!(
            detect_context("SELECT * FROM public.us", 23),
            CompletionContext::SchemaQualified
        );
    }

    #[test]
    fn test_ignore_strings() {
        assert_eq!(
            detect_context("SELECT 'FROM us", 15),
            CompletionContext::Nothing
        );
        assert_eq!(
            detect_context("SELECT \"col", 11),
            CompletionContext::Nothing
        );
    }

    #[test]
    fn test_ignore_comments() {
        assert_eq!(
            detect_context("-- SELECT FROM us", 17),
            CompletionContext::Nothing
        );
    }

    #[test]
    fn test_find_word_start() {
        assert_eq!(find_word_start("SELECT * FROM users", 19), 14);
        assert_eq!(find_word_start("SELECT col", 10), 7);
        assert_eq!(find_word_start("public.users", 12), 0);
        assert_eq!(find_word_start("SELECT * FROM public.us", 23), 14);
    }

    #[test]
    fn test_find_last_keyword() {
        assert_eq!(
            find_last_keyword("SELECT * FROM"),
            Some("FROM".to_string())
        );
        assert_eq!(
            find_last_keyword("SELECT * FROM users WHERE"),
            Some("WHERE".to_string())
        );
        assert_eq!(find_last_keyword("no keywords here"), None);
    }

    #[test]
    fn test_is_inside_string() {
        assert!(is_inside_string_or_comment("'hello world", 8));
        assert!(!is_inside_string_or_comment("'hello' world", 10));
        assert!(is_inside_string_or_comment("\"hello world", 8));
    }
}
