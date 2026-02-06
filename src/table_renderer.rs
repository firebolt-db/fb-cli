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
                // Color NULL values differently to distinguish from string "NULL"
                if val.is_null() {
                    Cell::new(display_value).fg(Color::DarkGrey)
                } else {
                    Cell::new(display_value)
                }
            })
            .collect();

        table.add_row(row_cells);
    }

    table.to_string()
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

/// Calculate the display width of a string, ignoring ANSI escape codes
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

                // Value cell - color NULL values differently to distinguish from string "NULL"
                let value_cell = if row[col_idx].is_null() {
                    Cell::new(truncated_value).fg(Color::DarkGrey)
                } else {
                    Cell::new(truncated_value)
                };

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

    #[test]
    fn test_null_rendering() {
        // Test that NULL values and string "NULL" are both rendered correctly
        // (Color distinction can be verified manually; tests just ensure no crashes)
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
            vec![Value::Number(1.into()), Value::Null], // Real NULL
            vec![Value::Number(2.into()), Value::String("NULL".to_string())], // String "NULL"
            vec![Value::Number(3.into()), Value::String("test".to_string())], // Regular string
        ];

        // Should not crash and should contain NULL text
        let output = render_table(&columns, &rows, 1000);
        assert!(output.contains("NULL"));
        assert!(output.contains("test"));
        assert!(output.contains('1'));
        assert!(output.contains('2'));
        assert!(output.contains('3'));
    }

    #[test]
    fn test_null_rendering_vertical() {
        // Test that NULL values are rendered in vertical mode without crashes
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
            vec![Value::Number(1.into()), Value::Null],
            vec![Value::Number(2.into()), Value::String("NULL".to_string())],
        ];

        // Should not crash and should contain NULL text
        let output = render_table_vertical(&columns, &rows, 80, 1000);
        assert!(output.contains("NULL"));
        assert!(output.contains("id"));
        assert!(output.contains("value"));
    }
}
