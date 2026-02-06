use comfy_table::{Attribute, Cell, Color, ColumnConstraint, ContentArrangement, Table, Width as ComfyWidth};
use serde::Deserialize;
use serde_json::Value;
use terminal_size::{terminal_size, Width};

#[derive(Debug, Deserialize)]
#[serde(tag = "message_type")]
pub enum JsonLineMessage {
    #[serde(rename = "START")]
    Start {
        result_columns: Vec<ResultColumn>,
        query_id: String,
        request_id: String,
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

pub fn render_table(columns: &[ResultColumn], rows: &[Vec<Value>], max_value_length: usize) -> String {
    let mut table = Table::new();

    // Enable dynamic content arrangement for automatic wrapping
    table.set_content_arrangement(ContentArrangement::Dynamic);

    // Detect terminal width and calculate equal column widths
    let terminal_width = terminal_size()
        .map(|(Width(w), _)| w)
        .unwrap_or(80);

    table.set_width(terminal_width);

    // Calculate equal column width if we have columns
    let num_columns = columns.len();
    if num_columns > 0 {
        // Subtract 4 for outer table borders, then divide equally
        let available_width = terminal_width.saturating_sub(4);
        let col_width = available_width / num_columns as u16;

        // Set explicit column constraints for equal widths
        let constraints: Vec<ColumnConstraint> = (0..num_columns)
            .map(|_| ColumnConstraint::UpperBoundary(ComfyWidth::Fixed(col_width)))
            .collect();
        table.set_constraints(constraints);
    }

    // Add headers with styling
    let header_cells: Vec<Cell> = columns
        .iter()
        .map(|col| Cell::new(&col.name).fg(Color::Cyan).add_attribute(Attribute::Bold))
        .collect();

    table.set_header(header_cells);

    // Add data rows
    for row in rows {
        let row_cells: Vec<Cell> = row
            .iter()
            .map(|val| {
                let value_str = format_value(val);
                // Truncate strings exceeding max_value_length
                let display_value = if value_str.len() > max_value_length {
                    format!("{}...", &value_str[..max_value_length.saturating_sub(3)])
                } else {
                    value_str
                };
                Cell::new(display_value)
            })
            .collect();

        table.add_row(row_cells);
    }

    table.to_string()
}

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

/// Calculate the display width of a string, ignoring ANSI escape codes
fn display_width(s: &str) -> usize {
    let mut width = 0;
    let mut in_escape = false;

    for ch in s.chars() {
        if ch == '\x1b' {
            // Start of ANSI escape sequence
            in_escape = true;
        } else if in_escape {
            // Inside escape sequence - check for end
            if ch.is_ascii_alphabetic() {
                // End of escape sequence (SGR codes end with a letter)
                in_escape = false;
            }
            // Don't count characters inside escape sequences
        } else {
            // Regular character - count it
            width += 1;
        }
    }

    width
}

/// Render table in vertical format (two-column table with column names and values)
/// Used when table is too wide for horizontal display in auto mode
pub fn render_table_vertical(
    columns: &[ResultColumn],
    rows: &[Vec<Value>],
    terminal_width: u16,
    max_value_length: usize,
) -> String {
    let mut output = String::new();

    for (row_idx, row) in rows.iter().enumerate() {
        // Row header
        output.push_str(&format!("Row {}:\n", row_idx + 1));

        // Create a two-column table for this row
        let mut table = Table::new();
        table.set_content_arrangement(ContentArrangement::Dynamic);

        // Set column constraints to allow wrapping
        // First column (names): narrow, fixed
        // Second column (values): wide, allows wrapping
        let available_width = if terminal_width > 10 { terminal_width - 4 } else { 76 };
        table.set_constraints(vec![
            ColumnConstraint::UpperBoundary(ComfyWidth::Fixed(30)),  // Column names
            ColumnConstraint::UpperBoundary(ComfyWidth::Fixed(available_width.saturating_sub(30))),  // Values
        ]);

        // Add rows (no header - just column name | value pairs)
        for (col_idx, col) in columns.iter().enumerate() {
            if col_idx < row.len() {
                let value = format_value(&row[col_idx]);

                // Truncate long values
                let truncated_value = if value.len() > max_value_length {
                    format!("{}...", &value[..max_value_length])
                } else {
                    value
                };

                // Column name cell (cyan, bold)
                let name_cell = Cell::new(&col.name)
                    .fg(Color::Cyan)
                    .add_attribute(Attribute::Bold);

                // Value cell (default color)
                let value_cell = Cell::new(truncated_value);

                table.add_row(vec![name_cell, value_cell]);
            }
        }

        output.push_str(&table.to_string());

        // Blank line between rows (except after last row)
        if row_idx < rows.len() - 1 {
            output.push('\n');
            output.push('\n');
        }
    }

    output
}

pub fn should_use_vertical_mode(columns: &[ResultColumn], terminal_width: u16, min_col_width: usize) -> bool {
    let num_columns = columns.len();

    if num_columns == 0 {
        return false;
    }

    // Simple logic: switch to vertical if each column has less than min_col_width chars available
    let chars_per_column = (terminal_width as usize) / num_columns;
    chars_per_column < min_col_width
}

/// Format a Value for CSV output
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
    let header = columns
        .iter()
        .map(|col| escape_csv_field(&col.name))
        .collect::<Vec<_>>()
        .join(",");
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
    fn test_format_value_null() {
        assert_eq!(format_value(&Value::Null), "NULL");
    }

    #[test]
    fn test_format_value_string() {
        assert_eq!(format_value(&Value::String("test".to_string())), "test");
    }

    #[test]
    fn test_render_vertical_single_row() {
        let columns = vec![
            ResultColumn {
                name: "id".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "name".to_string(),
                column_type: "text".to_string(),
            },
            ResultColumn {
                name: "status".to_string(),
                column_type: "text".to_string(),
            },
        ];
        let rows = vec![vec![
            Value::Number(1.into()),
            Value::String("Alice".to_string()),
            Value::String("active".to_string()),
        ]];

        let output = render_table_vertical(&columns, &rows, 80, 1000);

        // Check for row header
        assert!(output.contains("Row 1:"));

        // Check for table format (contains column names)
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("status"));

        // Check for values
        assert!(output.contains("1"));
        assert!(output.contains("Alice"));
        assert!(output.contains("active"));

        // Should have table borders
        assert!(output.contains('+'));
        assert!(output.contains('|'));
    }

    #[test]
    fn test_render_vertical_multiple_rows() {
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

        let output = render_table_vertical(&columns, &rows, 80, 1000);

        // Should have both row headers
        assert!(output.contains("Row 1:"));
        assert!(output.contains("Row 2:"));

        // Should have both names
        assert!(output.contains("Alice"));
        assert!(output.contains("Bob"));

        // Should have blank line between rows (two consecutive newlines between tables)
        let parts: Vec<&str> = output.split("Row 2:").collect();
        assert!(parts.len() == 2);
        // Check there's spacing before "Row 2:"
        assert!(parts[0].ends_with("\n\n") || parts[0].ends_with("\n+"));
    }

    #[test]
    fn test_render_vertical_value_truncation() {
        let columns = vec![
            ResultColumn {
                name: "long_col".to_string(),
                column_type: "text".to_string(),
            },
        ];
        let long_value = "a".repeat(2000); // 2000 characters
        let rows = vec![vec![Value::String(long_value)]];

        let output = render_table_vertical(&columns, &rows, 80, 1000);

        // Should be truncated to 1000 chars + "..."
        assert!(output.contains("..."));

        // Value should not exceed max_value_length
        let lines: Vec<&str> = output.lines().collect();
        for line in lines {
            // Each line in the table shouldn't be excessively long
            assert!(line.len() < 1100); // Some margin for table borders
        }
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
    fn test_should_use_vertical_mode() {
        let columns = vec![
            ResultColumn {
                name: "col1".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "col2".to_string(),
                column_type: "text".to_string(),
            },
        ];

        // Wide terminal, few columns -> horizontal
        // 150 width / 2 columns = 75 chars per column >= 10, stay horizontal
        assert!(!should_use_vertical_mode(&columns, 150, 10));

        // Many columns -> vertical
        // 150 width / 20 columns = 7.5 chars per column < 10, use vertical
        let many_columns: Vec<ResultColumn> = (0..20)
            .map(|i| ResultColumn {
                name: format!("column_name_{}", i),
                column_type: "int".to_string(),
            })
            .collect();
        assert!(should_use_vertical_mode(&many_columns, 150, 10));

        // Narrow terminal with few columns -> vertical
        // 40 width / 5 columns = 8 chars per column < 10, use vertical
        let five_columns = vec![
            ResultColumn {
                name: "a".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "b".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "c".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "d".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "e".to_string(),
                column_type: "int".to_string(),
            },
        ];
        assert!(should_use_vertical_mode(&five_columns, 40, 10));

        // Configurable threshold test
        // 80 width / 10 columns = 8 chars per column
        let ten_columns: Vec<ResultColumn> = (0..10)
            .map(|i| ResultColumn {
                name: format!("col{}", i),
                column_type: "int".to_string(),
            })
            .collect();
        // With threshold 8, should stay horizontal (8 >= 8)
        assert!(!should_use_vertical_mode(&ten_columns, 80, 8));
        // With threshold 9, should switch to vertical (8 < 9)
        assert!(should_use_vertical_mode(&ten_columns, 80, 9));
    }

    #[test]
    fn test_vertical_mode_threshold() {
        // Test that the decision is based purely on terminal_width / num_columns
        let three_columns: Vec<ResultColumn> = (0..3)
            .map(|i| ResultColumn {
                name: format!("col{}", i),
                column_type: "text".to_string(),
            })
            .collect();

        // 80 width / 3 columns = 26.6 chars per column >= 10, stay horizontal
        assert!(!should_use_vertical_mode(&three_columns, 80, 10));

        // But with a higher threshold of 30, should switch to vertical (26.6 < 30)
        assert!(should_use_vertical_mode(&three_columns, 80, 30));

        // Edge case: exactly at threshold
        let eight_columns: Vec<ResultColumn> = (0..8)
            .map(|i| ResultColumn {
                name: format!("c{}", i),
                column_type: "int".to_string(),
            })
            .collect();
        // 80 width / 8 columns = 10 chars per column
        // Should stay horizontal (10 >= 10)
        assert!(!should_use_vertical_mode(&eight_columns, 80, 10));
        // Should switch to vertical (10 < 11)
        assert!(should_use_vertical_mode(&eight_columns, 80, 11));
    }

    #[test]
    fn test_display_width() {
        // Plain text
        assert_eq!(display_width("hello"), 5);

        // Text with ANSI color codes (cyan)
        assert_eq!(display_width("\x1b[36mhello\x1b[0m"), 5);

        // Text with ANSI bold + color
        assert_eq!(display_width("\x1b[1m\x1b[36mhello\x1b[0m"), 5);

        // Empty string
        assert_eq!(display_width(""), 0);

        // Only ANSI codes
        assert_eq!(display_width("\x1b[36m\x1b[1m\x1b[0m"), 0);
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
