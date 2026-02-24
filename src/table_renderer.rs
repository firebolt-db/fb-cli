use crate::tui_msg::{TuiColor, TuiLine, TuiSpan};
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Deserialize)]
#[serde(tag = "message_type")]
pub enum JsonLineMessage {
    #[serde(rename = "START")]
    Start {
        result_columns: Vec<ResultColumn>,
        // The following fields are part of the Firebolt JSONLines_Compact protocol
        // and required for deserialization, but not used by the client renderer.
        #[allow(dead_code)]
        query_id: String,
        #[allow(dead_code)]
        request_id: String,
        #[allow(dead_code)]
        query_label: Option<String>,
    },
    #[serde(rename = "DATA")]
    Data { data: Vec<Vec<Value>> },
    #[serde(rename = "FINISH_SUCCESSFULLY")]
    FinishSuccessfully { statistics: Option<Value> },
    #[serde(rename = "FINISH_WITH_ERRORS")]
    FinishWithErrors { errors: Vec<ErrorDetail> },
}

#[derive(Clone, Debug, Deserialize)]
pub struct ResultColumn {
    pub name: String,
    // Column type from Firebolt server (e.g., "bigint", "integer", "text").
    // Currently unused for rendering but available for future type-aware formatting.
    // Required for deserialization.
    #[allow(dead_code)]
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct ErrorDetail {
    pub description: String,
}

#[derive(Clone, Debug)]
pub struct ParsedResult {
    pub columns: Vec<ResultColumn>,
    pub rows: Vec<Vec<Value>>,
    pub statistics: Option<Value>,
    pub errors: Option<Vec<ErrorDetail>>,
}

pub fn parse_jsonlines_compact(text: &str) -> Result<ParsedResult, Box<dyn std::error::Error>> {
    let mut columns: Vec<ResultColumn> = Vec::new();
    let mut all_rows: Vec<Vec<Value>> = Vec::new();
    let mut statistics: Option<Value> = None;
    let mut errors: Option<Vec<ErrorDetail>> = None;

    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }

        let message: JsonLineMessage = serde_json::from_str(trimmed)?;

        match message {
            JsonLineMessage::Start { result_columns, .. } => {
                columns = result_columns;
            }
            JsonLineMessage::Data { data } => {
                all_rows.extend(data);
            }
            JsonLineMessage::FinishSuccessfully { statistics: stats } => {
                statistics = stats;
            }
            JsonLineMessage::FinishWithErrors { errors: errs } => {
                errors = Some(errs);
            }
        }
    }

    Ok(ParsedResult {
        columns,
        rows: all_rows,
        statistics,
        errors,
    })
}


/// Format a serde_json::Value for table display.
///
/// Handles Firebolt's JSONLines_Compact serialization format where different
/// Firebolt types are serialized to JSON in specific ways:
///
/// **JSON Numbers** (rendered via `.to_string()`):
/// - INT, DOUBLE, REAL → JSON numbers (e.g., `42`, `3.14`)
///
/// **JSON Strings** (rendered as-is without quotes):
/// - BIGINT → JSON strings to preserve precision (e.g., `"9223372036854775807"`)
/// - NUMERIC/DECIMAL → JSON strings for exact decimals (e.g., `"1.23"`)
/// - TEXT → JSON strings (e.g., `"regular text"`)
/// - DATE → ISO format strings (e.g., `"2026-02-06"`)
/// - TIMESTAMP → ISO-like strings with timezone (e.g., `"2026-02-06 15:35:34+00"`)
/// - BYTEA → Hex-encoded binary data (e.g., `"\\x48656c6c6f"`)
/// - GEOGRAPHY → WKB (Well-Known Binary) format in hex (e.g., `"0101000020E6..."`)
///
/// **Other JSON Types**:
/// - ARRAY → JSON arrays (e.g., `[1,2,3]`)
/// - BOOLEAN → JSON booleans (e.g., `true`, `false`)
/// - NULL → JSON `null`, rendered as "NULL" string for SQL-style display
///
/// Note: The `column_type` field in ResultColumn is available but currently unused.
/// It could be leveraged for future type-aware formatting (e.g., right-align numbers,
/// format dates differently).
fn format_value(value: &Value) -> String {
    match value {
        Value::Null => "NULL".to_string(),
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Array(_) | Value::Object(_) => {
            let json_str = serde_json::to_string(value).unwrap_or_else(|_| "".to_string());
            // Truncate very long JSON (e.g., query_telemetry with hundreds of KB)
            const MAX_JSON_LENGTH: usize = 1000;
            if json_str.len() > MAX_JSON_LENGTH {
                format!("{}... (truncated)", &json_str[..MAX_JSON_LENGTH])
            } else {
                json_str
            }
        }
    }
}


/// Format a serde_json::Value for CSV export.
///
/// Similar to format_value(), but with CSV-specific differences:
/// - `Null` → empty string "" (CSV standard for null values)
/// - Other types formatted identically to format_value()
///
/// Maintains Firebolt's serialization: BIGINT strings, NUMERIC strings, etc.
fn format_value_csv(val: &Value) -> String {
    match val {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Array(_) | Value::Object(_) => val.to_string(),
    }
}

/// Escape a CSV field according to RFC 4180
fn escape_csv_field(field: &str) -> String {
    // Escape fields containing comma, quote, or newline
    if field.contains(',') || field.contains('"') || field.contains('\n') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Convert ParsedResult to CSV format
pub fn write_result_as_csv<W: std::io::Write>(
    writer: &mut W,
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
) -> Result<(), Box<dyn std::error::Error>> {
    // Write CSV header
    let header = columns.iter().map(|col| escape_csv_field(&col.name)).collect::<Vec<_>>().join(",");
    writeln!(writer, "{}", header)?;

    // Write data rows
    for row in rows {
        let row_str = row
            .iter()
            .map(|val| escape_csv_field(&format_value_csv(val)))
            .collect::<Vec<_>>()
            .join(",");
        writeln!(writer, "{}", row_str)?;
    }

    Ok(())
}

// ── TUI-native table renderers ────────────────────────────────────────────────
// These produce Vec<TuiLine> directly, so the TUI can apply ratatui styles
// without any ANSI round-trip.

/// Wrap a string (by char boundaries) into lines of at most `width` chars each,
/// padding every line to exactly `width` chars with spaces.
fn wrap_cell(s: &str, width: usize) -> Vec<String> {
    if width == 0 {
        return vec![String::new()];
    }
    let chars: Vec<char> = s.chars().collect();
    if chars.is_empty() {
        return vec![" ".repeat(width)];
    }
    let mut lines = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let end = (start + width).min(chars.len());
        let slice: String = chars[start..end].iter().collect();
        let padded = format!("{:<width$}", slice, width = width);
        lines.push(padded);
        start = end;
    }
    lines
}

/// Build a TUI border line using box-drawing characters.
/// Each column occupies `col_width + 2` chars (1 space padding on each side).
fn make_tui_border(
    left: &str,
    fill: &str,
    mid: &str,
    right: &str,
    col_widths: &[usize],
) -> TuiLine {
    let mut s = String::from(left);
    for (i, &w) in col_widths.iter().enumerate() {
        // fill repeated for col_width + 2 (padding on each side)
        for _ in 0..w + 2 {
            s.push_str(fill);
        }
        if i < col_widths.len() - 1 {
            s.push_str(mid);
        }
    }
    s.push_str(right);
    TuiLine(vec![TuiSpan::plain(s)])
}

/// Decide column widths for horizontal layout.
/// Returns None if vertical format should be used instead.
fn decide_col_widths(
    columns: &[ResultColumn],
    formatted: &[Vec<String>],
    terminal_width: usize,
    max_value_length: usize,
) -> Option<Vec<usize>> {
    let n = columns.len();
    if n == 0 {
        return Some(vec![]);
    }

    // Overhead for N columns: 3*N + 1  (for "│ col │ col │" structure)
    let overhead = 3 * n + 1;
    let available = terminal_width.saturating_sub(overhead);

    // Single-column special case: always horizontal.
    if n == 1 {
        let effective_max = max_value_length.max(10_000);
        let natural = {
            let h = columns[0].name.chars().count();
            let d = formatted.iter().map(|r| r[0].chars().count()).max().unwrap_or(0);
            h.max(d)
        };
        // Cap natural width at (terminal_width * 4 / 5).max(10)
        let cap = (terminal_width * 4 / 5).max(10);
        let natural_capped = natural.min(cap);
        let col_w = (terminal_width.saturating_sub(4)).max(1).min(natural_capped).max(1);
        let _ = effective_max; // used conceptually for max_value_length
        return Some(vec![col_w]);
    }

    // Compute natural width for each column (capped at (terminal_width * 4/5).max(10))
    let cap = (terminal_width * 4 / 5).max(10);
    let natural_widths: Vec<usize> = (0..n)
        .map(|i| {
            let h = columns[i].name.chars().count();
            let d = formatted.iter().map(|r| r[i].chars().count()).max().unwrap_or(0);
            h.max(d).min(cap)
        })
        .collect();

    let sum_natural: usize = natural_widths.iter().sum();

    // If sum of natural widths <= available: use horizontal with natural widths.
    if sum_natural <= available {
        return Some(natural_widths);
    }

    // If available / N < 10: use vertical format.
    if available / n < 10 {
        return None;
    }

    // Compute minimum width per column:
    // - At least 10
    // - Header wraps at most once: ceil(header_len / 2)
    // - Content not taller than wide: ceil(sqrt(max_content_len))
    let min_widths: Vec<usize> = (0..n)
        .map(|i| {
            let header_len = columns[i].name.chars().count();
            let max_content = formatted.iter().map(|r| r[i].chars().count()).max().unwrap_or(0);
            let min_from_header = (header_len + 1) / 2; // ceil(header_len / 2)
            let min_from_content = (max_content as f64).sqrt().ceil() as usize;
            10usize.max(min_from_header).max(min_from_content)
        })
        .collect();

    let sum_min: usize = min_widths.iter().sum();

    // If sum of minimum widths > available: use vertical format.
    if sum_min > available {
        return None;
    }

    // Distribute remaining space: start with min_widths, distribute extra space.
    let mut col_widths = min_widths.clone();
    let mut remaining = available - sum_min;

    // Each pass adds 1 to each column that hasn't reached its natural width.
    loop {
        if remaining == 0 {
            break;
        }
        let mut added = 0usize;
        for i in 0..n {
            if remaining == 0 {
                break;
            }
            if col_widths[i] < natural_widths[i] {
                col_widths[i] += 1;
                remaining -= 1;
                added += 1;
            }
        }
        if added == 0 {
            // All columns at natural width.
            break;
        }
    }

    // Switch to vertical if any cell needs more wrap rows than there are columns.
    // A tall narrow cell is a sign that vertical layout will be much more readable.
    let any_cell_too_tall = formatted.iter().any(|row| {
        (0..n).any(|i| {
            let len = row[i].chars().count();
            let w = col_widths[i].max(1);
            let rows_needed = (len + w - 1) / w;
            rows_needed > n
        })
    });
    if any_cell_too_tall {
        return None;
    }

    Some(col_widths)
}

/// Render a horizontal table with given column widths as TUI lines.
/// Handles multi-line cell wrapping with box-drawing borders.
fn render_horizontal_tui(
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
    formatted: &[Vec<String>],
    col_widths: &[usize],
) -> Vec<TuiLine> {
    let n = columns.len();
    if n == 0 {
        return vec![];
    }

    // Check if any cell (header or data) needs wrapping.
    let any_wrap = {
        let header_wraps = (0..n).any(|i| columns[i].name.chars().count() > col_widths[i]);
        let data_wraps = formatted.iter().any(|row| (0..n).any(|i| row[i].chars().count() > col_widths[i]));
        header_wraps || data_wraps
    };

    let mut lines = Vec::new();

    // Top border: ┌──────┬──────┐
    lines.push(make_tui_border("┌", "─", "┬", "┐", col_widths));

    // Header row (multi-line aware).
    let header_cells: Vec<Vec<String>> = (0..n)
        .map(|i| wrap_cell(&columns[i].name, col_widths[i]))
        .collect();
    let header_height = header_cells.iter().map(|c| c.len()).max().unwrap_or(1);
    for line_idx in 0..header_height {
        let mut spans: Vec<TuiSpan> = vec![TuiSpan::plain("│ ")];
        for (i, cell_lines) in header_cells.iter().enumerate() {
            let text = cell_lines.get(line_idx).cloned().unwrap_or_else(|| " ".repeat(col_widths[i]));
            spans.push(TuiSpan::styled(text, TuiColor::Cyan, true));
            if i < n - 1 {
                spans.push(TuiSpan::plain(" │ "));
            }
        }
        spans.push(TuiSpan::plain(" │"));
        lines.push(TuiLine(spans));
    }

    // Header separator: ╞═══════╪═══════╡
    lines.push(make_tui_border("╞", "═", "╪", "╡", col_widths));

    // Data rows.
    for (row_idx, row_cells) in formatted.iter().enumerate() {
        let null_flags: Vec<bool> = (0..n)
            .map(|i| rows.get(row_idx).and_then(|r| r.get(i)).map(|v| v.is_null()).unwrap_or(false))
            .collect();

        let wrapped: Vec<Vec<String>> = (0..n)
            .map(|i| wrap_cell(&row_cells[i], col_widths[i]))
            .collect();
        let row_height = wrapped.iter().map(|c| c.len()).max().unwrap_or(1);

        for line_idx in 0..row_height {
            let mut spans: Vec<TuiSpan> = vec![TuiSpan::plain("│ ")];
            for (i, cell_lines) in wrapped.iter().enumerate() {
                let text = cell_lines.get(line_idx).cloned().unwrap_or_else(|| " ".repeat(col_widths[i]));
                if null_flags[i] && line_idx == 0 {
                    spans.push(TuiSpan::styled(text, TuiColor::DarkGray, false));
                } else if null_flags[i] {
                    // Padding lines for NULL cells also in dark gray
                    spans.push(TuiSpan::styled(text, TuiColor::DarkGray, false));
                } else {
                    spans.push(TuiSpan::plain(text));
                }
                if i < n - 1 {
                    spans.push(TuiSpan::plain(" │ "));
                }
            }
            spans.push(TuiSpan::plain(" │"));
            lines.push(TuiLine(spans));
        }

        // Row separator between data rows (only if any cell wraps).
        if any_wrap && row_idx < formatted.len() - 1 {
            lines.push(make_tui_border("├", "─", "┼", "┤", col_widths));
        }
    }

    // Bottom border: └──────┴──────┘
    lines.push(make_tui_border("└", "─", "┴", "┘", col_widths));

    lines
}

/// Truncate a formatted value to at most `max_len` chars (char count), appending "..." if truncated.
fn truncate_to_chars(s: String, max_len: usize) -> String {
    let char_count = s.chars().count();
    if char_count > max_len {
        let truncated: String = s.chars().take(max_len.saturating_sub(3)).collect();
        format!("{}...", truncated)
    } else {
        s
    }
}

/// Format and truncate a cell value, using char-count for length measurement.
/// Control characters (newlines, tabs, carriage returns) are replaced with spaces
/// so that wrap_cell produces clean fixed-width chunks that ratatui renders correctly.
fn fmt_cell(val: &Value, max_value_length: usize) -> String {
    let s = format_value(val);
    // Replace control characters that would break ratatui's line rendering.
    let s = s.chars().map(|c| if c.is_control() { ' ' } else { c }).collect::<String>();
    truncate_to_chars(s, max_value_length)
}

/// Render a table in auto mode (horizontal if it fits, vertical otherwise).
/// Uses smart column width computation and box-drawing borders.
pub fn render_table_to_tui_lines(
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
    terminal_width: u16,
    max_value_length: usize,
) -> Vec<TuiLine> {
    if columns.is_empty() {
        return vec![];
    }
    let n = columns.len();
    let tw = terminal_width as usize;

    // For single-column tables, use a larger max to show more content.
    let effective_max = if n == 1 { max_value_length.max(10_000) } else { max_value_length };

    // Format all cell values using char-count for truncation.
    let formatted: Vec<Vec<String>> = rows
        .iter()
        .map(|row| (0..n).map(|i| fmt_cell(row.get(i).unwrap_or(&Value::Null), effective_max)).collect())
        .collect();

    match decide_col_widths(columns, &formatted, tw, effective_max) {
        Some(col_widths) => render_horizontal_tui(columns, rows, &formatted, &col_widths),
        None => render_vertical_table_to_tui_lines(columns, rows, terminal_width, max_value_length),
    }
}

/// Render a vertical (two-column: name | value) table as TUI lines.
/// For each original row, renders a mini 2-column table with column names and values.
pub fn render_vertical_table_to_tui_lines(
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
    terminal_width: u16,
    max_value_length: usize,
) -> Vec<TuiLine> {
    let tw = terminal_width as usize;

    // Name column width: max of all column name lengths, clamped to [10, 30].
    let max_name_len = columns.iter().map(|c| c.name.chars().count()).max().unwrap_or(0);
    let name_col_w = max_name_len.min(30).max(10);

    // Value column width: remaining space minus overhead (2 cols = 3*2+1 = 7 overhead).
    let val_col_w = tw.saturating_sub(name_col_w + 7).max(10);

    let col_widths = [name_col_w, val_col_w];

    let mut lines = Vec::new();

    for (row_idx, row) in rows.iter().enumerate() {
        // "Row N:" label (cyan + bold) as a standalone TuiLine.
        lines.push(TuiLine(vec![TuiSpan::styled(
            format!("Row {}:", row_idx + 1),
            TuiColor::Cyan,
            true,
        )]));

        // Check if any name or value wraps within this row's mini-table.
        let any_wrap_in_row = columns.iter().enumerate().any(|(col_idx, col)| {
            let name_wraps = col.name.chars().count() > name_col_w;
            let val = fmt_cell(row.get(col_idx).unwrap_or(&Value::Null), max_value_length);
            let val_wraps = val.chars().count() > val_col_w;
            name_wraps || val_wraps
        });

        // Top border of mini-table.
        lines.push(make_tui_border("┌", "─", "┬", "┐", &col_widths));

        for (field_idx, col) in columns.iter().enumerate() {
            let is_null = row.get(field_idx).map(|v| v.is_null()).unwrap_or(false);
            let val = fmt_cell(row.get(field_idx).unwrap_or(&Value::Null), max_value_length);

            let name_lines = wrap_cell(&col.name, name_col_w);
            let val_lines = wrap_cell(&val, val_col_w);
            let field_height = name_lines.len().max(val_lines.len());

            for line_idx in 0..field_height {
                let name_text = name_lines.get(line_idx).cloned().unwrap_or_else(|| " ".repeat(name_col_w));
                let val_text = val_lines.get(line_idx).cloned().unwrap_or_else(|| " ".repeat(val_col_w));

                let mut spans: Vec<TuiSpan> = vec![TuiSpan::plain("│ ")];
                spans.push(TuiSpan::styled(name_text, TuiColor::Cyan, true));
                spans.push(TuiSpan::plain(" │ "));
                if is_null {
                    spans.push(TuiSpan::styled(val_text, TuiColor::DarkGray, false));
                } else {
                    spans.push(TuiSpan::plain(val_text));
                }
                spans.push(TuiSpan::plain(" │"));
                lines.push(TuiLine(spans));
            }

            // Row separator between fields (only if any name or value wraps).
            if any_wrap_in_row && field_idx < columns.len() - 1 {
                lines.push(make_tui_border("├", "─", "┼", "┤", &col_widths));
            }
        }

        // Bottom border of mini-table.
        lines.push(make_tui_border("└", "─", "┴", "┘", &col_widths));

        // Blank line between rows (except after last).
        if row_idx < rows.len() - 1 {
            lines.push(TuiLine(vec![TuiSpan::plain(String::new())]));
        }
    }

    lines
}

/// Render a table always in horizontal format (forced horizontal).
/// If column widths would normally cause vertical layout, falls back to equal-share widths.
pub fn render_horizontal_forced_tui_lines(
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
    terminal_width: u16,
    max_value_length: usize,
) -> Vec<TuiLine> {
    if columns.is_empty() {
        return vec![];
    }
    let n = columns.len();
    let tw = terminal_width as usize;

    let effective_max = if n == 1 { max_value_length.max(10_000) } else { max_value_length };

    let formatted: Vec<Vec<String>> = rows
        .iter()
        .map(|row| (0..n).map(|i| fmt_cell(row.get(i).unwrap_or(&Value::Null), effective_max)).collect())
        .collect();

    let col_widths = match decide_col_widths(columns, &formatted, tw, effective_max) {
        Some(widths) => widths,
        None => {
            // Fall back to equal-share column widths.
            let overhead = 3 * n + 1;
            let available = tw.saturating_sub(overhead);
            let equal_w = (available / n).max(1);
            vec![equal_w; n]
        }
    };

    render_horizontal_tui(columns, rows, &formatted, &col_widths)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_jsonlines() {
        let input = r#"{"message_type":"START","query_id":"123","request_id":"456","query_label":null,"result_columns":[{"name":"col1","type":"integer"},{"name":"col2","type":"text"}]}
{"message_type":"DATA","data":[[1,"hello"],[2,"world"]]}
{"message_type":"FINISH_SUCCESSFULLY","statistics":{"elapsed":0.123}}"#;

        let result = parse_jsonlines_compact(input).unwrap();
        assert_eq!(result.columns.len(), 2);
        assert_eq!(result.rows.len(), 2);
        assert!(result.errors.is_none());
    }

    #[test]
    fn test_parse_multiple_data_messages() {
        let input = r#"{"message_type":"START","query_id":"123","request_id":"456","query_label":null,"result_columns":[{"name":"id","type":"integer"}]}
{"message_type":"DATA","data":[[1],[2]]}
{"message_type":"DATA","data":[[3],[4]]}
{"message_type":"FINISH_SUCCESSFULLY","statistics":{}}"#;

        let result = parse_jsonlines_compact(input).unwrap();
        assert_eq!(result.rows.len(), 4);
    }

    #[test]
    fn test_parse_with_errors() {
        let input = r#"{"message_type":"START","query_id":"123","request_id":"456","query_label":null,"result_columns":[]}
{"message_type":"FINISH_WITH_ERRORS","errors":[{"description":"Syntax error"}]}"#;

        let result = parse_jsonlines_compact(input).unwrap();
        assert!(result.errors.is_some());
        assert_eq!(result.errors.unwrap()[0].description, "Syntax error");
    }

    #[test]
    fn test_format_value_firebolt_bigint() {
        // BIGINT arrives as JSON string (not number) to preserve precision
        let bigint_max = Value::String("9223372036854775807".to_string());
        assert_eq!(format_value(&bigint_max), "9223372036854775807");

        let bigint_min = Value::String("-9223372036854775808".to_string());
        assert_eq!(format_value(&bigint_min), "-9223372036854775808");

        // Should display as-is without quotes
        let output = format_value(&bigint_max);
        assert!(!output.starts_with('"'), "BIGINT should not have quotes in display");
    }

    #[test]
    fn test_format_value_firebolt_numeric() {
        // NUMERIC/DECIMAL arrives as JSON string for exact precision
        let decimal = Value::String("1.23".to_string());
        assert_eq!(format_value(&decimal), "1.23");

        let large_decimal = Value::String("12345678901234567890.123456789".to_string());
        assert_eq!(format_value(&large_decimal), "12345678901234567890.123456789");
    }

    #[test]
    fn test_format_value_firebolt_integers() {
        // INT arrives as JSON number
        let int_val = Value::Number(42.into());
        assert_eq!(format_value(&int_val), "42");

        let negative = Value::Number((-42).into());
        assert_eq!(format_value(&negative), "-42");

        // INT min/max values
        let int_max = Value::Number(2147483647.into());
        assert_eq!(format_value(&int_max), "2147483647");

        let int_min = Value::Number((-2147483648).into());
        assert_eq!(format_value(&int_min), "-2147483648");
    }

    #[test]
    fn test_format_value_firebolt_floats() {
        // DOUBLE/REAL arrive as JSON numbers
        let pi = serde_json::Number::from_f64(3.14159).unwrap();
        let formatted = format_value(&Value::Number(pi));
        assert!(formatted.starts_with("3.14"), "Got: {}", formatted);

        // Integer-valued float keeps decimal point
        let one = serde_json::Number::from_f64(1.0).unwrap();
        assert_eq!(format_value(&Value::Number(one)), "1.0");
    }

    #[test]
    fn test_format_value_firebolt_temporal() {
        // DATE arrives as ISO format string
        let date = Value::String("2026-02-06".to_string());
        assert_eq!(format_value(&date), "2026-02-06");

        // TIMESTAMP arrives as ISO-like format with timezone
        let timestamp = Value::String("2026-02-06 15:35:34.519403+00".to_string());
        assert_eq!(format_value(&timestamp), "2026-02-06 15:35:34.519403+00");
    }

    #[test]
    fn test_format_value_firebolt_arrays() {
        // ARRAY arrives as JSON array
        let int_array = serde_json::json!([1, 2, 3]);
        let formatted = format_value(&int_array);
        assert!(formatted.contains("1"));
        assert!(formatted.contains("2"));
        assert!(formatted.contains("3"));

        // Empty array
        let empty = Value::Array(vec![]);
        assert_eq!(format_value(&empty), "[]");
    }

    #[test]
    fn test_format_value_firebolt_text() {
        // Regular TEXT
        let text = Value::String("regular text".to_string());
        assert_eq!(format_value(&text), "regular text");

        // TEXT with JSON-like content (still a string, not parsed)
        let json_text = Value::String("{\"key\": \"value\"}".to_string());
        assert_eq!(format_value(&json_text), "{\"key\": \"value\"}");

        // Unicode text
        let unicode = Value::String("🔥 emoji text".to_string());
        assert_eq!(format_value(&unicode), "🔥 emoji text");
    }

    #[test]
    fn test_format_value_firebolt_bytea() {
        // BYTEA arrives as hex-encoded string
        let bytea = Value::String("\\x48656c6c6f".to_string());
        assert_eq!(format_value(&bytea), "\\x48656c6c6f");

        // Should display as-is without interpretation
        let output = format_value(&bytea);
        assert!(output.starts_with("\\x"), "BYTEA should preserve hex encoding");
    }

    #[test]
    fn test_format_value_firebolt_geography() {
        // GEOGRAPHY arrives as WKB (Well-Known Binary) format in hex
        let geo_point = Value::String("0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040".to_string());
        assert_eq!(format_value(&geo_point), "0101000020E6100000FEFFFFFFFFFFEF3F0000000000000040");

        // Should display as hex string without interpretation
        let output = format_value(&geo_point);
        assert!(output.len() > 20, "GEOGRAPHY hex strings are long");
        assert!(!output.starts_with('"'), "Should not have quotes in display");
    }

    #[test]
    fn test_format_value_firebolt_null_and_bool() {
        // NULL rendered as "NULL" string for clarity
        assert_eq!(format_value(&Value::Null), "NULL");

        // BOOLEAN rendered as lowercase
        assert_eq!(format_value(&Value::Bool(true)), "true");
        assert_eq!(format_value(&Value::Bool(false)), "false");
    }

    #[test]
    fn test_format_value_csv_null_handling() {
        // In CSV, NULL should be empty string (not "NULL")
        assert_eq!(format_value_csv(&Value::Null), "");

        // But in table format, NULL should be "NULL"
        assert_eq!(format_value(&Value::Null), "NULL");

        // BIGINT strings should work in CSV too
        let bigint = Value::String("9223372036854775807".to_string());
        assert_eq!(format_value_csv(&bigint), "9223372036854775807");
    }

    #[test]
    fn test_format_value_null() {
        assert_eq!(format_value(&Value::Null), "NULL");
    }

    #[test]
    fn test_format_value_string() {
        assert_eq!(format_value(&Value::String("test".to_string())), "test");
    }

    #[test]
    fn test_json_truncation() {
        // Create a very large JSON array
        let large_array = Value::Array(vec![Value::String("x".repeat(500)); 10]);
        let rows = vec![vec![large_array]];

        let formatted = format_value(&rows[0][0]);

        // Should be truncated
        assert!(formatted.contains("truncated") || formatted.len() < 5000);
    }

    #[test]
    fn test_write_result_as_csv() {
        let columns = vec![
            ResultColumn {
                name: "id".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "name".to_string(),
                column_type: "text".to_string(),
            },
        ];
        let rows = vec![
            vec![Value::Number(1.into()), Value::String("Alice".to_string())],
            vec![Value::Number(2.into()), Value::String("Bob".to_string())],
        ];

        let mut output = Vec::new();
        write_result_as_csv(&mut output, &columns, &rows).unwrap();

        let csv_str = String::from_utf8(output).unwrap();
        assert!(csv_str.contains("id,name"));
        assert!(csv_str.contains("1,Alice"));
        assert!(csv_str.contains("2,Bob"));
    }

    #[test]
    fn test_csv_escaping() {
        let columns = vec![ResultColumn {
            name: "col".to_string(),
            column_type: "text".to_string(),
        }];
        let rows = vec![
            vec![Value::String("has,comma".to_string())],
            vec![Value::String("has\"quote".to_string())],
            vec![Value::String("has\nnewline".to_string())],
        ];

        let mut output = Vec::new();
        write_result_as_csv(&mut output, &columns, &rows).unwrap();

        let csv_str = String::from_utf8(output).unwrap();
        assert!(csv_str.contains("\"has,comma\""));
        assert!(csv_str.contains("\"has\"\"quote\""));
        assert!(csv_str.contains("\"has\nnewline\""));
    }

}
