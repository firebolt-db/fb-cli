use once_cell::sync::Lazy;
use pest::Parser;
use pest_derive::Parser;
use regex::Regex;
use std::time::Instant;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;

use crate::args::normalize_extras;
use crate::auth::authenticate_service_account;
use crate::context::Context;
use crate::utils::spin;
use crate::FIREBOLT_PROTOCOL_VERSION;
use crate::USER_AGENT;

// Set parameters via query
pub fn set_args(context: &mut Context, query: &str) -> Result<bool, Box<dyn std::error::Error>> {
    // set flag = value;
    static SET_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"(?i)^(?:--[^\n]*\n|/\*[\s\S]*\*/|[ \t\n])*set +([^ ]+?) *= *(.*?)\n*;?\n*$"#).unwrap());
    let matches_set = SET_RE.captures(&query);
    if matches_set.is_none() {
        return Ok(false);
    }

    let matches = matches_set.unwrap();
    let key = matches.get(1).unwrap().as_str();
    let value = matches.get(2).unwrap().as_str();

    if key == "format" {
        context.args.format = String::from(value);
    } else {
        let mut buf: Vec<String> = vec![];
        buf.push(format!("{key}={value}"));
        buf = normalize_extras(buf, true)?;
        context.args.extra.append(&mut buf);
        buf.append(&mut context.args.extra);
        context.args.extra = normalize_extras(buf, false)?;

        if !context.args.concise && key == "engine" && value == "system" {
            eprintln!("\nTo query SYSTEM engine please run 'unset engine'\n");
        }
    }

    context.update_url();

    return Ok(true);
}

// Unset parameters via query
pub fn unset_args(context: &mut Context, query: &str) -> Result<bool, Box<dyn std::error::Error>> {
    static UNSET_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r#"(?i)^(?:--[^\n]*\n|/\*[\s\S]*\*/|[ \t\n])*unset +([^ ]+?)\s*(--.*)?\n*;?\n*$"#).unwrap());
    if let Some(matches) = UNSET_RE.captures(&query) {
        let key = matches.get(1).unwrap().as_str();
        let prefix = format!("{key}=");
        context.args.extra.retain(|e| !e.starts_with(prefix.as_str()));
        if key == "format" {
            context.args.format = String::from("PSQL");
        } else if key == "database" {
            context.args.database = String::from("");
        }

        context.update_url();

        return Ok(true);
    }

    Ok(false)
}

// Send query and print result.
pub async fn query(context: &mut Context, query_text: String) -> Result<(), Box<dyn std::error::Error>> {
    // Handle set/unset commands
    if set_args(context, &query_text)? {
        if !context.args.concise && !context.args.hide_pii {
            eprintln!("URL: {}", context.url);
        }

        return Ok(());
    }

    if unset_args(context, &query_text)? {
        if !context.args.concise && !context.args.hide_pii {
            eprintln!("URL: {}", context.url);
        }

        return Ok(());
    }

    if !context.args.sa_id.is_empty() || !context.args.sa_secret.is_empty() {
        authenticate_service_account(context).await?;
    }

    if context.args.verbose {
        eprintln!("URL: {}", context.url);
        eprintln!("QUERY: {}", query_text);
    }

    let start = Instant::now();

    let mut request = reqwest::Client::builder()
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3600))
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(60)))
        .http2_keep_alive_while_idle(false)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .build()?
        .post(context.url.clone())
        .header("user-agent", USER_AGENT)
        .header("Firebolt-Protocol-Version", FIREBOLT_PROTOCOL_VERSION)
        .body(query_text);

    if let Some(sa_token) = &context.sa_token {
        request = request.header("authorization", format!("Bearer {}", sa_token.token));
    }

    if !context.args.jwt.is_empty() {
        request = request.header("authorization", format!("Bearer {}", context.args.jwt));
    }

    let async_resp = request.send();

    let finish_token = CancellationToken::new();
    let maybe_spin = if context.args.no_spinner || context.args.concise {
        None
    } else {
        let token_clone = finish_token.clone();
        Some(task::spawn(async {
            spin(token_clone).await;
        }))
    };

    select! {
        _ = signal::ctrl_c() => {
            finish_token.cancel();
            if let Some(spin) = maybe_spin {
                spin.await?;
            }
            if !context.args.concise {
                eprintln!("^C");
            }
        }
        response = async_resp => {
            let elapsed = start.elapsed();
            finish_token.cancel();
            if let Some(spin) = maybe_spin {
                spin.await?;
            }

            let mut maybe_request_id: Option<String> = None;
            match response {
                Ok(resp) => {
                    let mut updated_url = false;
                    for (header, value) in resp.headers() {
                        if header == "firebolt-remove-parameters" {
                            unset_args(context, format!("unset {}", value.to_str()?).as_str())?;
                            updated_url = true;
                        } else if header == "firebolt-update-parameters" {
                            set_args(context, format!("set {}", value.to_str()?).as_str())?;
                            updated_url = true;
                        } else if header == "X-REQUEST-ID" {
                            maybe_request_id = value.to_str().map_or(None, |l| Some(String::from(l)));
                            updated_url = true;
                        } else if header == "firebolt-update-endpoint" {
                            let header_str = value.to_str()?;
                            // Split the header at the '?' character
                            if let Some(pos) = header_str.find('?') {
                                // Extract base URL and query part
                                let base_url = &header_str[..pos];
                                let query_part = &header_str[pos+1..];

                                // Update the context URL with just the base part
                                context.args.host = base_url.to_string();

                                // Process each query parameter
                                for param in query_part.split('&') {
                                    if !param.is_empty() {
                                        set_args(context, format!("set {};", param).as_str())?;
                                    }
                                }
                            } else {
                                // No query parameters, just set the URL
                                context.args.host = header_str.to_string();
                            }
                            updated_url = true;
                        }
                    }
                    if updated_url &&  !context.args.concise && !context.args.hide_pii {
                        eprintln!("URL: {}", context.url);
                    }

                    // on stdout, on purpose
                    println!("{}", resp.text().await?);
                }
                Err(error) => {
                    if context.args.verbose {
                        eprintln!("Failed to send the request: {:?}", error);
                    } else {
                        eprintln!("Failed to send the request: {}", error.to_string());
                    }
                },
            };

            if !context.args.concise {
                let elapsed = format!("{:?}", elapsed / 100000 * 100000);
                print!("Time: {elapsed}\n");
                if let Some(request_id) = maybe_request_id {
                    print!("Request Id: {request_id}\n");
                }
                // on stdout, on purpose
                println!("")
            }
        }
    };

    Ok(())
}

#[derive(Parser)]
#[grammar = "sql.pest"]
struct SQLParser;

pub fn try_split_queries(s: &str) -> Option<Vec<String>> {
    match SQLParser::parse(Rule::queries, s) {
        Ok(pairs) => {
            let queries: Vec<String> = pairs
                .into_iter()
                .next()? // Get the queries rule
                .into_inner() // Get inner rules (individual queries)
                .filter(|pair| pair.as_rule() == Rule::query)
                .filter(|pair| pair.as_str().len() > 1)
                .map(|pair| pair.as_str().to_string())
                .collect();

            if queries.is_empty() {
                None
            } else {
                Some(queries)
            }
        }
        Err(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::get_args;

    #[tokio::test]
    async fn test_query() {
        let mut args = get_args().unwrap();
        args.host = "localhost:8123".to_string();
        args.database = "test_db".to_string();

        let mut context = Context::new(args);
        let query_text = "select 42".to_string();

        let result = query(&mut context, query_text).await;
        assert!(result.is_ok());
    }

    #[test]
    fn test_basic_queries() {
        // Simple queries
        let input = "SELECT 1; SELECT 2;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " SELECT 2;");

        // Empty queries are ignored
        let input = "SELECT 1;;; SELECT 2;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " SELECT 2;");

        // Query must end with semicolon
        let input = "SELECT 1; SELECT 2";
        assert!(try_split_queries(input).is_none());

        // Empty input
        assert!(try_split_queries("").is_none());
        assert!(try_split_queries("   \n   ").is_none());
        assert!(try_split_queries(";").is_none());
        assert!(try_split_queries(";;;").is_none());
    }

    #[test]
    fn test_string_literals() {
        // Single quotes
        let input = "SELECT 'string with '' escaped quote'; SELECT 'normal string';";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 'string with '' escaped quote';");
        assert_eq!(queries[1], " SELECT 'normal string';");

        // Double quotes
        let input = "SELECT \"identifier with \"\" escaped quote\"; SELECT \"normal identifier\";";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // E-strings
        let input = "SELECT E'escaped \\'quote\\' here'; SELECT E'normal\\\\backslash';";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // Semicolons in strings don't split queries
        let input = "SELECT 'string with ; semicolon'";
        assert!(try_split_queries(input).is_none());

        // Unterminated string
        let input = "SELECT 'unterminated";
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_line_comments() {
        // Basic line comments
        let input = "SELECT 1; -- comment here\nSELECT 2; -- another comment\n;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " -- comment here\nSELECT 2;");
        assert_eq!(queries[2], " -- another comment\n;");

        // Comment eating semicolon
        let input = "SELECT 1; -- comment\nSELECT 2 -- final comment;";
        assert!(try_split_queries(input).is_none());

        // Comment before semicolon
        let input = "SELECT 1; -- comment\nSELECT 2 -- final comment\n;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
    }

    #[test]
    fn test_block_comments() {
        // Basic block comments
        let input = "SELECT 1; /* comment */ SELECT 2; /* final comment */;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Nested comments
        let input = "SELECT /* outer /* nested */ comment */ 1; /* final comment */;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // Multiple nesting levels
        let input = "SELECT 1; /* l1 /* l2 /* l3 */ l2 */ l1 */ SELECT 2;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // Unterminated comment
        let input = "SELECT /* unterminated";
        assert!(try_split_queries(input).is_none());

        // Semicolons in comments don't split queries
        let input = "SELECT 1; /* comment ; with ; semicolons */ SELECT 2;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
    }

    #[test]
    fn test_comments_and_quotes() {
        // Comments in strings
        let input = r#"
            SELECT '-- not a comment' AS c1;
            SELECT '/* also not a comment */' AS c2;
            SELECT "-- not a comment either";
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Strings in comments
        let input = r#"
            SELECT 1; -- Here's a 'string'
            SELECT 2; /* And "another" one */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Complex whitespace
        let input = "SELECT 1;\n\n\tSELECT 2;\r\n  SELECT 3;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
    }

    #[test]
    fn test_complex_query() {
        let input = r#"
            -- Initial comment
            SELECT 'string with ;' AS c1, /* comment */ "id with ;" AS c2;

            /* Multi-line
               comment with ;
               semicolons */
            UPDATE /* nested /* comments */ */ table
            SET col = E'escaped \' quote' -- Comment
            WHERE id IN (
                SELECT id -- Subquery
                FROM other_table
            );

            -- Final query
            DELETE FROM table /* comment */ WHERE id = 1 /* comment */;
            -- Final comment
            /* Final block comment */
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(
            queries[0],
            "\n            -- Initial comment\n            SELECT 'string with ;' AS c1, /* comment */ \"id with ;\" AS c2;"
        );
        assert_eq!(queries[1], "\n\n            /* Multi-line\n               comment with ;\n               semicolons */\n            UPDATE /* nested /* comments */ */ table\n            SET col = E'escaped \\' quote' -- Comment\n            WHERE id IN (\n                SELECT id -- Subquery\n                FROM other_table\n            );");
        assert_eq!(
            queries[2],
            "\n\n            -- Final query\n            DELETE FROM table /* comment */ WHERE id = 1 /* comment */;"
        );
    }

    #[test]
    fn test_set_args() {
        let args = get_args().unwrap();
        let mut context = Context::new(args);

        // Test setting format
        let query = "set format = TSV";
        let result = set_args(&mut context, query).unwrap();
        assert!(result);
        assert_eq!(context.args.format, "TSV");

        // Test setting engine parameter
        let query = "set engine = default";
        let result = set_args(&mut context, query).unwrap();
        assert!(result);
        assert!(context.args.extra.iter().any(|e| e == "engine=default"));

        // Test with comments before SET command
        let query = "-- Setting a parameter\nset test = value";
        let result = set_args(&mut context, query).unwrap();
        assert!(result);
        assert!(context.args.extra.iter().any(|e| e.starts_with("test=")));
    }

    #[test]
    fn test_unset_args() {
        let mut args = get_args().unwrap();
        args.format = "TSV".to_string();
        args.extra.push("engine=default".to_string());

        let mut context = Context::new(args);

        // Test unsetting engine parameter
        let query = "unset engine";
        let result = unset_args(&mut context, query).unwrap();
        assert!(result);
        assert!(!context.args.extra.iter().any(|e| e.starts_with("engine=")));

        // Test unsetting format
        let query = "unset format";
        let result = unset_args(&mut context, query).unwrap();
        assert!(result);
        assert_eq!(context.args.format, "PSQL");

        // Test with comments before UNSET command
        context.args.extra.push("test=value".to_string());
        let query = "/* Comment */\nunset test";
        let result = unset_args(&mut context, query).unwrap();
        assert!(result);
        assert!(!context.args.extra.iter().any(|e| e.starts_with("test=")));
    }

    #[test]
    fn test_semicolon() {
        // Valid queries must end with semicolon
        let input = "SELECT 1; SELECT 2; SELECT 3;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Query without final semicolon is invalid
        let input = "SELECT 1; SELECT 2; SELECT 3";
        assert!(try_split_queries(input).is_none());

        // Empty queries (just semicolons) should be ignored
        let input = ";;;";
        assert!(try_split_queries(input).is_none());

        // Whitespace after semicolon is allowed
        let input = "SELECT 1;   \n  SELECT 2;  \t  ";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
    }

    #[test]
    fn test_comments_after_semicolon() {
        // Single line comments after semicolon
        let input = "SELECT 1; -- comment\nSELECT 2; -- final comment";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " -- comment\nSELECT 2;");

        // Single line comments eating semicolon
        let input = "SELECT 1; -- comment\nSELECT 2 -- final comment;";
        assert!(try_split_queries(input).is_none());

        // Single line comments before semicolon
        let input = "SELECT 1; -- comment\nSELECT 2 -- final comment\n;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " -- comment\nSELECT 2 -- final comment\n;");

        // Mixed comments after semicolon with proper termination
        let input = "SELECT 1; -- first comment\nSELECT 2; /* second comment */;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " -- first comment\nSELECT 2;");
        assert_eq!(queries[2], " /* second comment */;");

        // Block comments before semicolon
        let input = "SELECT 1;/* comment */ SELECT 2 /* final comment */;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], "/* comment */ SELECT 2 /* final comment */;");

        // Multiple comments after final semicolon
        let input = r#"SELECT 1;
            -- Comment 1
            /* Comment 2 */
            -- Comment 3
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT 1;");

        // Comments with semicolons inside them should not count as query terminators
        let input = r#"SELECT 1; /* comment ; with ; semicolons */ SELECT 2;
            -- Another ; comment
            /* Final ; comment */;"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0], "SELECT 1;");
        assert_eq!(queries[1], " /* comment ; with ; semicolons */ SELECT 2;");
        assert_eq!(queries[2], "\n            -- Another ; comment\n            /* Final ; comment */;");
    }

    #[test]
    fn test_nested_comments_with_semicolons() {
        // Nested block comments with semicolons
        let input = r#"
            SELECT 1; /* outer /* nested ; */ comment ; */ SELECT 2;
            /* Final comment */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
        assert_eq!(queries[0], "\n            SELECT 1;");
        assert_eq!(queries[1], " /* outer /* nested ; */ comment ; */ SELECT 2;");
        assert_eq!(queries[2], "\n            /* Final comment */;");

        // Unterminated nested comment
        let input = r#"
            SELECT 1; /* outer /* nested */ comment */ SELECT 2;
            /* Unterminated /* nested */ comment
        "#;
        assert!(try_split_queries(input).is_none());

        // Multiple nesting levels
        let input = r#"
            SELECT 1; /* l1 /* l2 /* l3 */ l2 */ l1 */ SELECT 2;
            /* Final comment */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
    }

    #[test]
    fn test_mixed_comments_and_quotes() {
        // Comments and quotes interleaved
        let input = r#"
            SELECT 'string /* not a comment */';
            SELECT "identifier -- not a comment";
            -- Comment 'not a string'
            /* Comment "not an identifier" */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Comments inside string literals
        let input = r#"
            SELECT '-- not a comment' AS c1;
            SELECT '/* also not a comment */' AS c2;
            -- Real comment
            /* Another real comment */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // E-string variants
        let input = r#"
            SELECT E'escaped \' quote';
            SELECT E'-- not a comment';
            -- Real comment
            /* Another comment */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
    }

    #[test]
    fn test_whitespace_handling() {
        // Various whitespace between queries
        let input = "SELECT 1;\n\n\tSELECT 2;\r\n  SELECT 3;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Only whitespace after final semicolon
        let input = "SELECT 1;\nSELECT 2;\n  \t  \n";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // Whitespace with comments
        let input = "SELECT 1;\n  -- Comment\n  \t/* Comment */  \nSELECT 2;";
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 2);

        // Mixed whitespace in comments
        let input = r#"
            SELECT 1; -- Comment with    spaces
            SELECT 2; /* Comment with
                multiple
                lines */;
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);
    }

    #[test]
    fn test_complex_scenarios() {
        // Complex query with everything mixed
        let input = r#"
            -- Initial comment
            SELECT 'string with ;' AS c1, /* comment */ "id with ;" AS c2;

            /* Multi-line
               comment with ;
               semicolons */
            UPDATE /* nested /* comments */ */ table
            SET col = E'escaped \' quote' -- Comment
            WHERE id IN (
                SELECT id -- Subquery
                FROM other_table
            );

            -- Final query
            DELETE FROM table /* comment */ WHERE id = 1 /* comment */;
            -- Final comment
            /* Final block comment */
        "#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 3);

        // Same query without final semicolon should fail
        let input = r#"
            SELECT 1;
            UPDATE table SET col = 'value';
            DELETE FROM table
        "#;
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_comments_with_semicolons() {
        let input = r#"SELECT 42 /*\nhello;"#;
        assert!(try_split_queries(input).is_none());

        let input = r#"SELECT 42 /*hello;"#;
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_strings_with_semicolons() {
        let input = r#"SELECT '42\nhello;"#;
        assert!(try_split_queries(input).is_none());

        let input = r#"SELECT '42;"#;
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_estrings_with_semicolons() {
        let input = r#"SELECT E'42\nhello;"#;
        assert!(try_split_queries(input).is_none());

        let input = r#"SELECT E'42;"#;
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_rawstrings_with_semicolons() {
        let input = r#"SELECT $$42\nhello;"#;
        assert!(try_split_queries(input).is_none());

        let input = r#"SELECT $$42;"#;
        assert!(try_split_queries(input).is_none());
    }

    #[test]
    fn test_empty_strings() {
        // Raw strings
        let input = r#"SELECT $$$$;"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT $$$$;");

        let input = r#"SELECT $$/* hi there; */$$;"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT $$/* hi there; */$$;");

        // Strings
        let input = r#"SELECT '';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT '';");

        let input = r#"SELECT '/* hi there; */';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT '/* hi there; */';");

        // E-strings
        let input = r#"SELECT E'';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT E'';");

        let input = r#"SELECT e'/* hi there; */';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], "SELECT e'/* hi there; */';");

        // Not strings, but let's do it here.
        let input = r#"SELECT "";"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT "";"#);

        let input = r#"SELECT "/* hi there; */";"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT "/* hi there; */";"#);
    }

    #[test]
    fn test_strings() {
        // Raw strings
        let input = r#"SELECT $$hello$$;"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT $$hello$$;"#);

        let input = r#"SELECT $$he\nllo$$;"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT $$he\nllo$$;"#);

        // Strings
        let input = r#"SELECT 'hello';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT 'hello';"#);

        // E-strings
        let input = r#"SELECT E'hello';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT E'hello';"#);

        let input = r#"SELECT e'he\nllo';"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT e'he\nllo';"#);

        // Not strings, but let's do it here.
        let input = r#"SELECT "hello";"#;
        let queries = try_split_queries(input).unwrap();
        assert_eq!(queries.len(), 1);
        assert_eq!(queries[0], r#"SELECT "hello";"#);
    }
}
