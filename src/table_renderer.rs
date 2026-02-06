use serde::Deserialize;
use serde_json::Value;
use comfy_table::{Table, Cell, Color, Attribute, ContentArrangement};
use terminal_size::{Width, terminal_size};

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
    Data {
        data: Vec<Vec<Value>>,
    },
    #[serde(rename = "FINISH_SUCCESSFULLY")]
    FinishSuccessfully {
        statistics: Option<Value>,
    },
    #[serde(rename = "FINISH_WITH_ERRORS")]
    FinishWithErrors {
        errors: Vec<ErrorDetail>,
    },
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

pub fn render_table(columns: &[ResultColumn], rows: &[Vec<Value>]) -> String {
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
        .map(|col| {
            Cell::new(&col.name)
                .fg(Color::Cyan)
                .add_attribute(Attribute::Bold)
        })
        .collect();

    table.set_header(header_cells);

    // Add data rows
    for row in rows {
        let row_cells: Vec<Cell> = row
            .iter()
            .map(|val| Cell::new(format_value(val)))
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
        Value::Array(_) | Value::Object(_) => serde_json::to_string(value).unwrap_or_else(|_| "".to_string()),
    }
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
}
