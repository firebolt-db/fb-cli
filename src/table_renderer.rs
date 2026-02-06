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

#[derive(Debug, Deserialize)]
pub struct ResultColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
}

#[derive(Debug, Deserialize)]
pub struct ErrorDetail {
    pub description: String,
}

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

    // Detect and set terminal width if available
    if let Some((Width(w), _)) = terminal_size() {
        table.set_width(w);
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

pub fn render_table_expanded(columns: &[ResultColumn], rows: &[Vec<Value>], terminal_width: u16, max_value_length: usize) -> String {
    const BORDER_OVERHEAD_PER_COL: usize = 3; // "│ " + " │"
    const MIN_COL_WIDTH: usize = 5; // Absolute minimum to avoid squashing

    let mut output = String::new();
    let available_width = terminal_width as usize;

    for (row_idx, row) in rows.iter().enumerate() {
        // Add row number header
        let row_header = format!("╔═══ Row {} ", row_idx + 1);
        let header_len = row_header.len();
        output.push_str(&row_header);
        output.push_str(&"═".repeat(terminal_width.saturating_sub(header_len as u16) as usize - 1));
        output.push_str("╗\n");

        // Calculate needed width for each column (max of column name and value)
        let mut col_widths: Vec<usize> = Vec::new();
        for (col_idx, col) in columns.iter().enumerate() {
            let col_name_width = col.name.len();
            let value_width = if col_idx < row.len() {
                let value_str = format_value(&row[col_idx]);
                let truncated = if value_str.len() > max_value_length {
                    max_value_length
                } else {
                    value_str.len()
                };
                truncated
            } else {
                0
            };
            // Use the larger of column name or value width, with a minimum
            col_widths.push(col_name_width.max(value_width).max(MIN_COL_WIDTH));
        }

        // Dynamically chunk columns based on actual width requirements
        let mut column_chunks: Vec<Vec<usize>> = Vec::new();
        let mut current_chunk: Vec<usize> = Vec::new();
        let mut current_width: usize = 4; // Start with table border overhead

        for (col_idx, &width) in col_widths.iter().enumerate() {
            let col_with_border = width + BORDER_OVERHEAD_PER_COL;

            // Check if adding this column would exceed available width
            if !current_chunk.is_empty() && (current_width + col_with_border > available_width) {
                // Start a new chunk
                column_chunks.push(current_chunk);
                current_chunk = vec![col_idx];
                current_width = 4 + col_with_border;
            } else {
                // Add to current chunk
                current_chunk.push(col_idx);
                current_width += col_with_border;
            }
        }

        // Don't forget the last chunk
        if !current_chunk.is_empty() {
            column_chunks.push(current_chunk);
        }

        for (chunk_idx, col_indices) in column_chunks.iter().enumerate() {
            // Reuse the widths we already calculated for chunking
            let min_widths: Vec<usize> = col_indices.iter().map(|&idx| col_widths[idx]).collect();

            // Calculate total content width needed
            let total_content_width: usize = min_widths.iter().sum();
            let num_cols = col_indices.len();

            // Total table width = borders (4) + columns content + column separators (3 per column)
            let total_min_width = 4 + total_content_width + (num_cols * 3);

            // Calculate extra space to distribute (make table exactly terminal_width)
            let target_total_width = available_width;
            let extra_space = if total_min_width < target_total_width {
                target_total_width - total_min_width
            } else {
                0
            };

            // Distribute extra space proportionally among columns
            let extra_per_col = if num_cols > 0 { extra_space / num_cols } else { 0 };
            let remainder = if num_cols > 0 { extra_space % num_cols } else { 0 };

            // Create a mini table for this chunk
            let mut table = Table::new();
            table.set_width(terminal_width);
            // Use Dynamic arrangement to ensure content wraps within the terminal width
            table.set_content_arrangement(ContentArrangement::Dynamic);

            // Add header row for this chunk
            let header_cells: Vec<Cell> = col_indices
                .iter()
                .map(|&idx| Cell::new(&columns[idx].name).fg(Color::Cyan).add_attribute(Attribute::Bold))
                .collect();
            table.set_header(header_cells);

            // Add value row for this chunk
            let value_cells: Vec<Cell> = col_indices
                .iter()
                .map(|&idx| {
                    let value_str = if idx < row.len() { format_value(&row[idx]) } else { String::new() };
                    // Truncate strings exceeding max_value_length
                    // Let comfy-table handle wrapping for reasonable lengths
                    let display_value = if value_str.len() > max_value_length {
                        format!("{}...", &value_str[..max_value_length.saturating_sub(3)])
                    } else {
                        value_str
                    };
                    Cell::new(display_value)
                })
                .collect();
            table.add_row(value_cells);

            // Set column constraints to ensure content fits within terminal width
            // For single-column chunks with very wide content, limit to a reasonable width
            if col_indices.len() == 1 && min_widths[0] > available_width / 2 {
                // Single column with very wide content - set max width to allow wrapping
                if let Some(column) = table.column_mut(0) {
                    // Use most of available width for the content column
                    let max_col_width = available_width.saturating_sub(10); // Leave room for borders
                    column.set_constraint(ColumnConstraint::UpperBoundary(ComfyWidth::Fixed(max_col_width as u16)));
                }
            } else {
                // Multiple columns or narrow content - set minimum width to prevent wrapping
                // but allow columns to be wider if needed for proper layout
                for (i, &min_width) in min_widths.iter().enumerate() {
                    let extra = extra_per_col + if i < remainder { 1 } else { 0 };
                    let target_width = min_width + extra;
                    if let Some(column) = table.column_mut(i) {
                        // Use LowerBoundary instead of fixed boundaries to prevent content wrapping
                        column.set_constraint(ColumnConstraint::LowerBoundary(ComfyWidth::Fixed(target_width as u16)));
                    }
                }
            }

            // Render this chunk
            let table_str = table.to_string();
            let lines: Vec<&str> = table_str.lines().collect();
            let num_lines = lines.len();

            // For the first chunk, skip the top border (we have our custom header)
            // For subsequent chunks, include everything
            let start_line = if chunk_idx == 0 { 1 } else { 0 };

            // Check if this is the last chunk
            let is_last_chunk = chunk_idx == column_chunks.len() - 1;

            for (line_idx, line) in lines.iter().enumerate() {
                if line_idx >= start_line {
                    // Skip the bottom border for non-last chunks since the next chunk's top border provides separation
                    if !is_last_chunk && line_idx == num_lines - 1 {
                        continue;
                    }

                    // Swap border characters for better visual hierarchy:
                    // - Use '-' for header separator (lighter, less prominent)
                    // - Use '=' for chunk separator at top of non-first chunks (heavier, more prominent)
                    let mut processed_line = line.to_string();
                    if line.starts_with('+') && line.contains('=') {
                        // Header separator line: change = to -
                        processed_line = processed_line.replace('=', "-");
                    } else if line.starts_with('+') && line.contains('-') && line_idx == 0 && chunk_idx > 0 {
                        // Top border of non-first chunks: change - to = to emphasize separation
                        processed_line = processed_line.replace('-', "=");
                    }

                    // Pad the line to terminal width for alignment
                    // Use display_width to ignore ANSI escape codes
                    let line_len = display_width(&processed_line);
                    if line_len < available_width {
                        // For lines ending with '+', extend with appropriate border character
                        let padded_line = if processed_line.ends_with('+') {
                            // Determine the border character from the line
                            let pad_char = if processed_line.contains('═') {
                                '═'
                            } else if processed_line.contains('=') {
                                '='
                            } else if processed_line.contains('-') {
                                '-'
                            } else {
                                '-'
                            };
                            let padding = pad_char.to_string().repeat(available_width - line_len);
                            format!("{}{}", &processed_line[..processed_line.len() - 1], padding) + "+"
                        } else {
                            // For content lines, pad with spaces
                            format!("{}{}", processed_line, " ".repeat(available_width - line_len))
                        };
                        output.push_str(&padded_line);
                    } else {
                        output.push_str(&processed_line);
                    }
                    output.push('\n');
                }
            }
        }

        // Add spacing between result rows for better visual separation
        if row_idx < rows.len() - 1 {
            output.push('\n');
            output.push('\n');
            output.push('\n');
        }
    }

    output
}

pub fn should_use_expanded_mode(columns: &[ResultColumn], rows: &[Vec<Value>], terminal_width: u16, max_value_length: usize) -> bool {
    const MAX_HORIZONTAL_COLUMNS: usize = 15;
    const BORDER_OVERHEAD_PER_COL: usize = 3;
    const MIN_COL_WIDTH: usize = 5;

    let num_columns = columns.len();
    let available_width = terminal_width as usize;

    // Rule 1: Too many columns (more than 15 is definitely too many for horizontal)
    if num_columns > MAX_HORIZONTAL_COLUMNS {
        return true;
    }

    // Rule 2: Content-aware check - calculate if all columns can fit horizontally
    // Use the same logic as expanded mode to calculate actual widths needed
    if !rows.is_empty() {
        let row = &rows[0];

        // Calculate needed width for each column (applying same truncation as rendering)
        let mut total_width = 4; // Table borders
        for (col_idx, col) in columns.iter().enumerate() {
            let col_name_width = col.name.len();
            let value_width = if col_idx < row.len() {
                let value_str = format_value(&row[col_idx]);
                // Apply the same truncation logic as rendering to be consistent
                if value_str.len() > max_value_length {
                    max_value_length
                } else {
                    value_str.len()
                }
            } else {
                0
            };
            let needed_width = col_name_width.max(value_width).max(MIN_COL_WIDTH);
            total_width += needed_width + BORDER_OVERHEAD_PER_COL;
        }

        // If all columns don't fit, use expanded mode
        if total_width > available_width {
            return true;
        }
    }

    false
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
    fn test_render_expanded_single_row() {
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

        let output = render_table_expanded(&columns, &rows, 80, 1000);

        // Check for row header
        assert!(output.contains("╔═══ Row 1"));

        // Check for column headers (should appear before values)
        assert!(output.contains("id"));
        assert!(output.contains("name"));
        assert!(output.contains("status"));

        // Check for values
        assert!(output.contains("Alice"));
        assert!(output.contains("active"));

        // Verify structure: headers should appear before values in the output
        let id_pos = output.find("id").unwrap();
        let alice_pos = output.find("Alice").unwrap();
        assert!(id_pos < alice_pos, "Column names should appear before values");
    }

    #[test]
    fn test_render_expanded_multiple_rows() {
        let columns = vec![
            ResultColumn {
                name: "id".to_string(),
                column_type: "int".to_string(),
            },
            ResultColumn {
                name: "value".to_string(),
                column_type: "text".to_string(),
            },
        ];
        let rows = vec![
            vec![Value::Number(1.into()), Value::String("A".to_string())],
            vec![Value::Number(2.into()), Value::String("B".to_string())],
        ];

        let output = render_table_expanded(&columns, &rows, 80, 1000);
        assert!(output.contains("╔═══ Row 1"));
        assert!(output.contains("╔═══ Row 2"));

        // Each row should have its own header line
        assert_eq!(output.matches("╔═══ Row").count(), 2);
    }

    #[test]
    fn test_value_truncation() {
        let columns = vec![
            ResultColumn {
                name: "short".to_string(),
                column_type: "text".to_string(),
            },
            ResultColumn {
                name: "very_long".to_string(),
                column_type: "text".to_string(),
            },
        ];

        // Create a very long string (>1000 chars) that should be truncated
        let long_string = "a".repeat(1500);
        let rows = vec![vec![Value::String("ok".to_string()), Value::String(long_string.clone())]];

        let output = render_table_expanded(&columns, &rows, 80, 1000);

        // The very long string should be truncated with "..."
        assert!(output.contains("..."));
        assert!(!output.contains(&long_string)); // Full string should not appear

        // But strings under 1000 chars should NOT be truncated (no "..." marker)
        let medium_string = "b".repeat(200);
        let rows2 = vec![vec![Value::String("ok".to_string()), Value::String(medium_string.clone())]];
        let output2 = render_table_expanded(&columns, &rows2, 80, 1000);

        // Medium string should not be truncated (no "..." marker)
        // Note: comfy-table may wrap it, so we just check it wasn't truncated
        assert!(!output2.contains("bbb..."));
        assert!(output2.contains("ok")); // At least verify basic rendering works
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
    fn test_should_use_expanded_mode() {
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
        let rows = vec![vec![Value::Number(1.into()), Value::String("test".to_string())]];

        // Wide terminal, few columns -> horizontal
        assert!(!should_use_expanded_mode(&columns, &rows, 150, 10000));

        // Many columns with longer names -> expanded
        let many_columns: Vec<ResultColumn> = (0..20)
            .map(|i| ResultColumn {
                name: format!("column_name_{}", i),
                column_type: "int".to_string(),
            })
            .collect();
        let many_cols_row = vec![vec![Value::Number(1.into()); 20]];
        assert!(should_use_expanded_mode(&many_columns, &many_cols_row, 150, 10000));

        // Narrow terminal with many columns -> expanded
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
        let five_cols_row = vec![vec![Value::Number(1.into()); 5]];
        assert!(should_use_expanded_mode(&five_columns, &five_cols_row, 40, 10000));
    }

    #[test]
    fn test_content_too_wide_detection() {
        let columns = vec![
            ResultColumn {
                name: "column_one_with_long_name".to_string(),
                column_type: "text".to_string(),
            },
            ResultColumn {
                name: "column_two_with_long_name".to_string(),
                column_type: "text".to_string(),
            },
            ResultColumn {
                name: "column_three_also_long".to_string(),
                column_type: "text".to_string(),
            },
        ];
        let rows = vec![vec![
            Value::String("this is a fairly long value".to_string()),
            Value::String("another long value here".to_string()),
            Value::String("and yet another long value".to_string()),
        ]];

        // Should detect that columns won't fit and use expanded mode
        assert!(should_use_expanded_mode(&columns, &rows, 80, 10000));
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
}
