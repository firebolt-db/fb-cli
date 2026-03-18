use crate::context::Context;
use crate::table_renderer::write_result_as_csv;
use crate::utils::temp_csv_path;
use std::fs::File;

/// Open csvlens viewer for the last query result
pub fn open_csvlens_viewer(context: &Context) -> Result<(), Box<dyn std::error::Error>> {
    // csvlens only works with client-side rendering (when last_result is populated)
    if !context.args.format.starts_with("client:") {
        return Err("csvlens viewer requires client-side rendering. Use --format client:auto or similar.".into());
    }

    // Check if we have a result to display
    let result = match &context.last_result {
        Some(r) => r,
        None => return Err("No query results to display. Run a query first.".into()),
    };

    // Check for errors in last result
    if let Some(ref errors) = result.errors {
        let msgs: Vec<&str> = errors.iter().map(|e| e.description.as_str()).collect();
        return Err(format!("Cannot display results — last query had errors: {}", msgs.join("; ")).into());
    }

    // Check if result is empty
    if result.columns.is_empty() {
        return Err("No data to display (no columns in result).".into());
    }

    if result.rows.is_empty() {
        return Err("Query returned 0 rows. Nothing to display.".into());
    }

    // Write result to temporary CSV file
    let csv_path = temp_csv_path()?;
    let mut file = File::create(&csv_path)?;
    write_result_as_csv(&mut file, &result.columns, &result.rows)?;
    drop(file); // Ensure file is flushed and closed

    // Launch csvlens viewer
    let csv_path_str = csv_path.to_string_lossy().to_string();
    let options = csvlens::CsvlensOptions {
        filename: Some(csv_path_str),
        ..Default::default()
    };

    let result = csvlens::run_csvlens_with_options(options);
    let _ = std::fs::remove_file(&csv_path);
    result.map(|_| ()).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::table_renderer::{ErrorDetail, ParsedResult, ResultColumn};
    use serde_json::Value;

    #[test]
    fn test_no_result_error() {
        let mut args = crate::args::get_args().unwrap();
        args.format = String::from("client:auto");
        let context = Context::new(args);

        let result = open_csvlens_viewer(&context);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("No query results"));
    }

    #[test]
    fn test_error_result() {
        let mut args = crate::args::get_args().unwrap();
        args.format = String::from("client:auto");
        let mut context = Context::new(args);
        context.last_result = Some(ParsedResult {
            columns: vec![],
            rows: vec![],
            statistics: None,
            errors: Some(vec![ErrorDetail {
                description: "Test error".to_string(),
            }]),
        });

        let result = open_csvlens_viewer(&context);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Test error"));
    }

    #[test]
    fn test_empty_columns() {
        let mut args = crate::args::get_args().unwrap();
        args.format = String::from("client:auto");
        let mut context = Context::new(args);
        context.last_result = Some(ParsedResult {
            columns: vec![],
            rows: vec![],
            statistics: None,
            errors: None,
        });

        let result = open_csvlens_viewer(&context);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("no columns"));
    }

    #[test]
    fn test_empty_rows() {
        let mut args = crate::args::get_args().unwrap();
        args.format = String::from("client:auto");
        let mut context = Context::new(args);
        context.last_result = Some(ParsedResult {
            columns: vec![ResultColumn {
                name: "col1".to_string(),
                column_type: "int".to_string(),
            }],
            rows: vec![],
            statistics: None,
            errors: None,
        });

        let result = open_csvlens_viewer(&context);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("0 rows"));
    }

    #[test]
    fn test_csv_file_creation() {
        let mut args = crate::args::get_args().unwrap();
        args.format = String::from("client:auto");
        let mut context = Context::new(args);
        context.last_result = Some(ParsedResult {
            columns: vec![
                ResultColumn {
                    name: "id".to_string(),
                    column_type: "int".to_string(),
                },
                ResultColumn {
                    name: "name".to_string(),
                    column_type: "text".to_string(),
                },
            ],
            rows: vec![
                vec![Value::Number(1.into()), Value::String("Alice".to_string())],
                vec![Value::Number(2.into()), Value::String("Bob".to_string())],
            ],
            statistics: None,
            errors: None,
        });

        // This test verifies that the CSV file is created and written correctly
        // We can't test the actual csvlens launch without a terminal, but we can
        // verify the file creation part
        let csv_path = temp_csv_path().unwrap();
        let mut file = File::create(&csv_path).unwrap();
        write_result_as_csv(
            &mut file,
            &context.last_result.as_ref().unwrap().columns,
            &context.last_result.as_ref().unwrap().rows,
        )
        .unwrap();
        drop(file);

        // Verify file exists and has content
        let content = std::fs::read_to_string(&csv_path).unwrap();
        assert!(content.contains("id,name"));
        assert!(content.contains("1,Alice"));
        assert!(content.contains("2,Bob"));

        // Clean up
        std::fs::remove_file(&csv_path).ok();
    }
}
