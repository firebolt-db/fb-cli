/// Detect whether the cursor is currently inside a SQL function call.
///
/// Walks the SQL text up to the cursor position, tracking open/close
/// parentheses with a stack.  Each `(` pushed records the identifier
/// immediately before it (the function name) or `None` for grouping
/// expressions.  Returns the innermost function name on the stack, if any.

/// Walk `sql[..cursor]` and return the name of the innermost function call
/// the cursor is currently inside, or `None`.
pub fn detect_function_at_cursor(sql: &str, cursor: usize) -> Option<String> {
    let bytes = sql.as_bytes();
    let end = cursor.min(bytes.len());
    // Stack of Option<String>: Some(name) for function calls, None for
    // plain grouping parens like `(1 + 2)`.
    let mut stack: Vec<Option<String>> = Vec::new();
    let mut i = 0;

    while i < end {
        match bytes[i] {
            // Single-quoted string
            b'\'' => {
                i += 1;
                while i < end {
                    match bytes[i] {
                        b'\'' => { i += 1; break; }
                        b'\\' => i += 2,
                        _ => i += 1,
                    }
                }
            }
            // Dollar-quoted string $$...$$
            b'$' if i + 1 < end && bytes[i + 1] == b'$' => {
                i += 2;
                while i + 1 < end {
                    if bytes[i] == b'$' && bytes[i + 1] == b'$' { i += 2; break; }
                    i += 1;
                }
            }
            // Double-quoted identifier
            b'"' => {
                i += 1;
                while i < end && bytes[i] != b'"' { i += 1; }
                if i < end { i += 1; }
            }
            // Line comment
            b'-' if i + 1 < end && bytes[i + 1] == b'-' => {
                while i < end && bytes[i] != b'\n' { i += 1; }
            }
            // Block comment
            b'/' if i + 1 < end && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < end {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' { i += 2; break; }
                    i += 1;
                }
            }
            b'(' => {
                let name = ident_before(sql, i);
                stack.push(name);
                i += 1;
            }
            b')' => {
                stack.pop();
                i += 1;
            }
            _ => i += 1,
        }
    }

    // Innermost function name (ignore grouping-paren Nones)
    stack.into_iter().rev().find_map(|x| x)
}

#[allow(dead_code)]
/// The byte offset in `sql` where the currently-active function name starts,
/// or `None`.  Used to position the popup horizontally.
pub fn func_name_start(sql: &str, cursor: usize) -> Option<usize> {
    let bytes = sql.as_bytes();
    let end = cursor.min(bytes.len());
    let mut stack: Vec<Option<(usize, String)>> = Vec::new(); // (name_start, name)
    let mut i = 0;

    while i < end {
        match bytes[i] {
            b'\'' => {
                i += 1;
                while i < end {
                    match bytes[i] {
                        b'\'' => { i += 1; break; }
                        b'\\' => i += 2,
                        _ => i += 1,
                    }
                }
            }
            b'$' if i + 1 < end && bytes[i + 1] == b'$' => {
                i += 2;
                while i + 1 < end {
                    if bytes[i] == b'$' && bytes[i + 1] == b'$' { i += 2; break; }
                    i += 1;
                }
            }
            b'"' => {
                i += 1;
                while i < end && bytes[i] != b'"' { i += 1; }
                if i < end { i += 1; }
            }
            b'-' if i + 1 < end && bytes[i + 1] == b'-' => {
                while i < end && bytes[i] != b'\n' { i += 1; }
            }
            b'/' if i + 1 < end && bytes[i + 1] == b'*' => {
                i += 2;
                while i + 1 < end {
                    if bytes[i] == b'*' && bytes[i + 1] == b'/' { i += 2; break; }
                    i += 1;
                }
            }
            b'(' => {
                let entry = ident_before_with_start(sql, i);
                stack.push(entry);
                i += 1;
            }
            b')' => { stack.pop(); i += 1; }
            _ => i += 1,
        }
    }

    stack.into_iter().rev().find_map(|x| x).map(|(start, _)| start)
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Extract the SQL identifier that ends immediately before byte position `pos`
/// (trimming trailing whitespace), lowercased.  Returns `None` for keywords
/// that open a sub-expression (`CASE`, `ARRAY`, …) or when nothing is found.
fn ident_before(sql: &str, pos: usize) -> Option<String> {
    ident_before_with_start(sql, pos).map(|(_, name)| name)
}

fn ident_before_with_start(sql: &str, pos: usize) -> Option<(usize, String)> {
    let before = &sql[..pos];
    let trimmed = before.trim_end();
    if trimmed.is_empty() { return None; }

    let end_byte = trimmed.len();
    // Walk backwards through the trimmed slice while chars are ident chars
    let mut start_byte = end_byte;
    for (byte_idx, ch) in trimmed.char_indices().rev() {
        if ch.is_alphanumeric() || ch == '_' {
            start_byte = byte_idx;
        } else {
            break;
        }
    }

    let name = &trimmed[start_byte..end_byte];
    if name.is_empty() || name.chars().next()?.is_ascii_digit() {
        return None;
    }

    // Filter out SQL keywords that look like identifiers but aren't function calls.
    // These are reserved words that can appear before `(` in non-call contexts.
    let upper = name.to_uppercase();
    if matches!(
        upper.as_str(),
        "SELECT" | "FROM" | "WHERE" | "JOIN" | "INNER" | "LEFT" | "RIGHT" | "FULL"
        | "OUTER" | "CROSS" | "ON" | "USING" | "GROUP" | "ORDER" | "HAVING"
        | "LIMIT" | "OFFSET" | "UNION" | "INTERSECT" | "EXCEPT" | "WITH"
        | "INSERT" | "INTO" | "UPDATE" | "SET" | "DELETE" | "CREATE" | "DROP"
        | "ALTER" | "TABLE" | "VIEW" | "INDEX" | "DATABASE" | "SCHEMA"
        | "CASE" | "WHEN" | "THEN" | "ELSE" | "END"
        | "IN" | "NOT" | "AND" | "OR" | "BETWEEN" | "LIKE" | "ILIKE"
        | "IS" | "AS" | "BY" | "AT" | "DISTINCT" | "ALL" | "EXISTS"
        | "PRIMARY" | "FOREIGN" | "KEY" | "REFERENCES" | "UNIQUE" | "CHECK"
        | "DEFAULT" | "CONSTRAINT" | "CASCADE" | "RESTRICT"
        | "BEGIN" | "COMMIT" | "ROLLBACK" | "TRANSACTION"
        | "EXPLAIN" | "DESCRIBE" | "SHOW" | "USE"
    ) {
        return None;
    }

    Some((start_byte, name.to_lowercase()))
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inside_simple_call() {
        assert_eq!(
            detect_function_at_cursor("SELECT upper(", 13),
            Some("upper".to_string())
        );
    }

    #[test]
    fn outside_after_close() {
        assert_eq!(detect_function_at_cursor("SELECT upper(x)", 15), None);
    }

    #[test]
    fn nested_call() {
        // cursor inside inner call: coalesce(upper(
        let sql = "SELECT coalesce(upper(";
        assert_eq!(
            detect_function_at_cursor(sql, sql.len()),
            Some("upper".to_string())
        );
    }

    #[test]
    fn between_calls() {
        // cursor after closing inner `)` but inside outer
        let sql = "SELECT coalesce(upper(x), ";
        assert_eq!(
            detect_function_at_cursor(sql, sql.len()),
            Some("coalesce".to_string())
        );
    }

    #[test]
    fn grouping_paren_ignored() {
        // `(1 + 2)` — no function name before `(`
        assert_eq!(detect_function_at_cursor("SELECT (1 + 2", 13), None);
    }

    #[test]
    fn inside_string_not_detected() {
        // The `(` is inside a string literal
        let sql = "SELECT 'upper(";
        assert_eq!(detect_function_at_cursor(sql, sql.len()), None);
    }

    #[test]
    fn multiline_sql() {
        let sql = "SELECT\n  upper(\n    col";
        assert_eq!(
            detect_function_at_cursor(sql, sql.len()),
            Some("upper".to_string())
        );
    }
}
