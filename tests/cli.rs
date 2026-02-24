use serde_json;
use std::io::Write;
use std::process::Command;

fn run_fb(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(args)
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();

    (output.status.success(), stdout, stderr)
}

/// Like run_fb but returns the exact exit code instead of a bool.
fn run_fb_code(args: &[&str]) -> (i32, String, String) {
    let output = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(args)
        .output()
        .expect("Failed to execute command");

    let stdout = String::from_utf8(output.stdout).unwrap();
    let stderr = String::from_utf8(output.stderr).unwrap();
    let code = output.status.code().unwrap_or(-1);

    (code, stdout, stderr)
}

#[test]
fn test_basic_query() {
    let (success, stdout, _) = run_fb(&["--core", "SELECT 42"]);
    assert!(success);
    assert!(stdout.contains("42"));
}

#[test]
fn test_set_format() {
    // First set format to TSV
    let (success, stdout, _) = run_fb(&["--core", "-f", "TabSeparatedWithNamesAndTypes", "SELECT 42;"]);
    assert!(success);
    assert_eq!(stdout, "?column?\nint\n42\n");
}

#[test]
fn test_interactive_mode() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "set format = TabSeparatedWithNamesAndTypes;").unwrap();
    writeln!(stdin, "SELECT 1;").unwrap();
    writeln!(stdin, "SELECT 2;").unwrap();
    writeln!(stdin, "SELECT 3; SELECT 4;").unwrap();
    writeln!(stdin, "unset database; select 5 || current_database();").unwrap();
    writeln!(stdin, "set database =; select 6 || current_database();").unwrap();
    drop(stdin); // Close stdin to end interactive mode

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success());
    assert!(stdout.contains("\n1\n"));
    assert!(stdout.contains("\n2\n"));
    assert!(stdout.contains("\n3\n"));
    assert!(stdout.contains("\n4\n"));
    assert!(stdout.contains("\n5firebolt\n"));
    assert!(stdout.contains("\n6firebolt\n"));
}

#[test]
fn test_params_escaping() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&[
            "--core",
            "-f",
            "TabSeparatedWithNamesAndTypes",
            "-e",
            r#"query_parameters={"name": "$1", "value": "a=}&"}"#,
        ])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT param('$1');").unwrap();
    writeln!(stdin, "SET advanced_mode=true;").unwrap();
    writeln!(stdin, "SELECT param('$1');").unwrap();
    writeln!(stdin, r#"SET query_parameters={{"name": "$1", "value": "b=}}&"}};"#).unwrap();
    writeln!(stdin, "SELECT param('$1');").unwrap();
    drop(stdin); // Close stdin to end interactive mode

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success());
    let mut lines = stdout.lines();
    // First query result
    assert_eq!(lines.next().unwrap(), "param");
    assert_eq!(lines.next().unwrap(), "text");
    assert_eq!(lines.next().unwrap(), "a=}&");
    // Second query result
    assert_eq!(lines.next().unwrap(), "param");
    assert_eq!(lines.next().unwrap(), "text");
    assert_eq!(lines.next().unwrap(), "a=}&");
    // Third query result
    assert_eq!(lines.next().unwrap(), "param");
    assert_eq!(lines.next().unwrap(), "text");
    assert_eq!(lines.next().unwrap(), "b=}&");
}

#[test]
fn test_argument_parsing_space_separated() {
    // Test space-separated argument format: --host localhost --database testdb
    let (success, _, stderr) = run_fb(&["--core", "--verbose", "--host", "localhost", "--database", "testdb", "SELECT 1"]);

    // The command should succeed (connection failure is expected, we're testing argument parsing)
    assert!(success || stderr.contains("Connection refused"));

    // Check that arguments were parsed correctly by looking at the URL in verbose output
    assert!(stderr.contains("database=testdb"));
    assert!(stderr.contains("http://localhost"));
}

#[test]
fn test_argument_parsing_equals_separated() {
    // Test equals-separated argument format: --host=localhost --database=testdb
    let (success, _, stderr) = run_fb(&["--core", "--verbose", "--host=localhost", "--database=testdb", "SELECT 1"]);

    // The command should succeed (connection failure is expected, we're testing argument parsing)
    assert!(success || stderr.contains("Connection refused"));

    // Check that arguments were parsed correctly by looking at the URL in verbose output
    assert!(stderr.contains("database=testdb"));
    assert!(stderr.contains("http://localhost"));
}

#[test]
fn test_argument_parsing_mixed_format() {
    // Test mixed argument format: --host=localhost --database testdb
    let (success, _, stderr) = run_fb(&["--core", "--verbose", "--host=localhost", "--database", "testdb", "SELECT 1"]);

    // The command should succeed (connection failure is expected, we're testing argument parsing)
    assert!(success || stderr.contains("Connection refused"));

    // Check that arguments were parsed correctly by looking at the URL in verbose output
    assert!(stderr.contains("database=testdb"));
    assert!(stderr.contains("http://localhost"));
}

#[test]
fn test_argument_parsing_short_options() {
    // Test that short options work with space separation (equals not supported for short options)
    let (success, _, stderr) = run_fb(&["--core", "--verbose", "-h", "localhost", "-d", "testdb", "SELECT 1"]);

    // The command should succeed (connection failure is expected, we're testing argument parsing)
    assert!(success || stderr.contains("Connection refused"));

    // Check that arguments were parsed correctly
    assert!(stderr.contains("database=testdb"));
    assert!(stderr.contains("http://localhost"));
}

#[test]
fn test_command_parsing() {
    // fb -c "select 1337"
    let (success, stdout, stderr) = run_fb(&["--core", "-c", "select 1337"]);

    assert!(success || stderr.contains("Connection refused"));

    assert!(stdout.contains("1337"));

    // fb -c select 1338
    let (success, stdout, stderr) = run_fb(&["--core", "-c", "select", "1338"]);

    assert!(success || stderr.contains("Connection refused"));

    assert!(stdout.contains("1338"));

    // fb select 1339
    let (success, stdout, stderr) = run_fb(&["--core", "select", "1339"]);

    assert!(success || stderr.contains("Connection refused"));

    assert!(stdout.contains("1339"));
}

#[test]
fn test_exiting() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core", "-f", "TabSeparatedWithNamesAndTypes"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 42;").unwrap();
    writeln!(stdin, "quit").unwrap();
    drop(stdin); // Close stdin to end interactive mode

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();

    assert!(output.status.success());
    let mut lines = stdout.lines();
    assert_eq!(lines.next().unwrap(), "?column?");
    lines.next();
    assert_eq!(lines.next().unwrap(), "42");
    lines.next();
}

#[test]
fn test_json_output_fully_parseable() {
    // Test that JSON output on stdout is fully parseable, even when stats are printed to stderr
    let (success, stdout, stderr) = run_fb(&["--core", "-f", "JSONLines_Compact", "SELECT 42 AS value"]);

    assert!(success);

    // server-side format: no stats on either stream
    assert!(!stderr.contains("Time:") && !stdout.contains("Time:"), "server-side format: no timing stats");

    // stdout should be valid JSON Lines - each non-empty line should be valid JSON
    let trimmed_stdout = stdout.trim();
    assert!(!trimmed_stdout.is_empty(), "stdout should not be empty");

    for line in trimmed_stdout.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(line);
        assert!(
            parsed.is_ok(),
            "Each line of stdout should be valid JSON, but got parse error: {:?}\nline was: {}\nfull stdout was: {}",
            parsed.err(),
            line,
            stdout
        );
    }
}

#[test]
fn test_exit_code_on_connection_error() {
    // Test that exit code is non-zero when server is not available
    let (success, _, stderr) = run_fb(&["--host", "localhost:59999", "SELECT 1"]);

    assert!(!success, "Exit code should be non-zero when connection fails");
    assert!(
        stderr.contains("Failed to send the request"),
        "stderr should contain connection error message, got: {}",
        stderr
    );
}

#[test]
fn test_exit_code_on_query_error() {
    // Test that exit code is non-zero when query returns an error (e.g., syntax error)
    let (success, stdout, stderr) = run_fb(&["--core", "SELEC INVALID SYNTAX"]);

    assert!(!success, "Exit code should be non-zero when query fails");
    // The server should return an error message in stdout or stderr
    let combined = format!("{}{}", stdout, stderr);
    assert!(
        combined.to_lowercase().contains("error") || combined.to_lowercase().contains("exception"),
        "output should contain error message from server, got stdout: {} stderr: {}",
        stdout, stderr
    );
}

#[test]
fn test_exit_code_on_query_error_interactive() {
    // Test that exit code is non-zero when any query fails in interactive mode
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 1;").unwrap(); // Valid query
    writeln!(stdin, "SELEC INVALID;").unwrap(); // Invalid query
    writeln!(stdin, "SELECT 2;").unwrap(); // Valid query
    drop(stdin);

    let output = child.wait_with_output().unwrap();

    assert!(
        !output.status.success(),
        "Exit code should be non-zero when any query in session fails"
    );
}

#[test]
fn test_auto_format() {
    let (success, stdout, _) = run_fb(&["--core", "--format=client:auto", "SELECT 1 as id, 'test' as name"]);
    assert!(success);
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
    assert!(stdout.contains("test"));
}

#[test]
fn test_expanded_format() {
    let (success, stdout, _) = run_fb(&["--core", "--format=client:vertical", "SELECT 1 as id, 'test' as name"]);
    assert!(success);
    assert!(stdout.contains("Row 1:"));
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
    assert!(stdout.contains("test"));
}

#[test]
fn test_wide_table_auto_expanded() {
    // Query with many columns — layout depends on terminal width, just verify data is present
    let (success, stdout, _) = run_fb(&[
        "--core",
        "--format=client:auto",
        "SELECT 1 as a, 2 as b, 3 as c, 4 as d, 5 as e, 6 as f, \
         7 as g, 8 as h, 9 as i, 10 as j, 11 as k, 12 as l, 13 as m",
    ]);
    assert!(success);
    assert!(stdout.contains('a') && stdout.contains('m')); // Column headers present
    assert!(stdout.contains('1') && stdout.contains("13")); // Values present
}

#[test]
fn test_narrow_table_stays_horizontal() {
    // Query with few columns should stay horizontal
    let (success, stdout, _) = run_fb(&["--core", "--format=client:auto", "SELECT 1 as id, 'test' as name"]);
    assert!(success);
    assert!(!stdout.contains("Row 1:")); // Should NOT use vertical
    assert!(stdout.contains("id")); // But still contains data
}

#[test]
fn test_client_format_horizontal() {
    let (success, stdout, _) = run_fb(&["--core", "--format=client:horizontal", "SELECT 1 as id, 'test' as name"]);
    assert!(success);

    // Should have horizontal table format
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
    assert!(stdout.contains("test"));
    assert!(stdout.contains('│')); // Has column separators (Unicode box-drawing)

    // Should NOT use vertical format
    assert!(!stdout.contains("Row 1"));
}

#[test]
fn test_client_format_vertical() {
    let (success, stdout, _) = run_fb(&["--core", "--format=client:vertical", "SELECT 1 as id, 'test' as name"]);
    assert!(success);

    // Should have vertical format
    assert!(stdout.contains("Row 1"));
    assert!(stdout.contains("id"));
    assert!(stdout.contains("name"));
}

#[test]
fn test_client_format_auto() {
    // Auto should choose based on terminal width
    let (success, stdout, _) = run_fb(&["--core", "--format=client:auto", "SELECT 1 as id"]);
    assert!(success);

    // Should have table format
    assert!(stdout.contains('│')); // Has table borders (Unicode box-drawing)
    assert!(stdout.contains("id"));
}

#[test]
fn test_server_format_json() {
    // Server-side format (no client: prefix)
    let (success, stdout, _) = run_fb(&["--core", "--format=JSON_Compact", "SELECT 1 as id"]);
    assert!(success);

    // Should have JSON format from server
    assert!(stdout.contains('{')); // JSON
    assert!(!stdout.contains('+')); // Not a table
}

#[test]
fn test_server_format_psql() {
    let (success, stdout, _) = run_fb(&["--core", "--format=PSQL", "SELECT 1 as id"]);
    assert!(success);

    // Should have PSQL format from server
    assert!(!stdout.contains('+')); // No table borders (PSQL style is different)
    assert!(stdout.contains("id"));
}

// ── Exit code precision ──────────────────────────────────────────────────────

#[test]
fn test_exit_code_query_error_is_1() {
    let (code, _, _) = run_fb_code(&["--core", "SELEC INVALID SYNTAX"]);
    assert_eq!(code, 1, "bad SQL should exit with code 1, not {}", code);
}

#[test]
fn test_exit_code_system_error_is_2() {
    let (code, _, stderr) = run_fb_code(&["--host", "localhost:59999", "SELECT 1"]);
    assert_eq!(code, 2, "connection error should exit with code 2, not {}; stderr: {}", code, stderr);
}

// ── exit command ─────────────────────────────────────────────────────────────

#[test]
fn test_exit_command() {
    // 'exit' should work the same as 'quit'
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core", "-f", "TabSeparatedWithNamesAndTypes"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 99;").unwrap();
    writeln!(stdin, "exit").unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success());
    assert!(stdout.contains("99"));
}

// ── Default format ───────────────────────────────────────────────────────────

#[test]
fn test_default_format_is_client_auto() {
    // Without --format, output should use client:auto (bordered table)
    let (success, stdout, _) = run_fb(&["--core", "SELECT 1 as id"]);
    assert!(success);
    assert!(stdout.contains('│'), "default format should produce table borders (client:auto)");
    assert!(stdout.contains("id"));
}

// ── stdout / stderr separation ───────────────────────────────────────────────

#[test]
fn test_stats_on_stderr_not_stdout() {
    // Server-side format: no stats on either stream (clean output for scripting)
    let (success, stdout, stderr) = run_fb(&["--core", "-f", "TabSeparatedWithNamesAndTypes", "SELECT 42"]);
    assert!(success);
    assert!(!stdout.contains("Time:"), "server-side format: no timing on stdout");
    assert!(!stderr.contains("Time:"), "server-side format: no timing on stderr");

    // Client-side format: stats follow the table on stdout
    let (success, stdout, _) = run_fb(&["--core", "SELECT 42"]);
    assert!(success);
    assert!(stdout.contains("Time:"), "client-side format: timing should be on stdout");
}

// ── Scripting output formats ─────────────────────────────────────────────────

#[test]
fn test_json_compact_output() {
    let (success, stdout, _) = run_fb(&["--core", "--format=JSON_Compact", "SELECT 1 AS n"]);
    assert!(success);
    let parsed: serde_json::Value = serde_json::from_str(stdout.trim())
        .expect("JSON_Compact output should be valid JSON");
    assert!(parsed.get("data").is_some(), "JSON_Compact should have a 'data' field");
    assert!(parsed.get("meta").is_some(), "JSON_Compact should have a 'meta' field");
}

#[test]
fn test_tsv_output() {
    let (success, stdout, _) = run_fb(&["--core", "--format=TabSeparatedWithNamesAndTypes", "SELECT 42 AS answer"]);
    assert!(success);
    assert!(stdout.contains("answer"), "TabSeparatedWithNamesAndTypes should include header");
    assert!(stdout.contains("42"));
}

// ── Pipe mode ────────────────────────────────────────────────────────────────

#[test]
fn test_pipe_mode_multiple_queries_in_order() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core", "-f", "TabSeparatedWithNamesAndTypes"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 10;").unwrap();
    writeln!(stdin, "SELECT 20;").unwrap();
    writeln!(stdin, "SELECT 30;").unwrap();
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success());
    // All three results should appear in order
    let pos10 = stdout.find("10").expect("first result missing");
    let pos20 = stdout.find("20").expect("second result missing");
    let pos30 = stdout.find("30").expect("third result missing");
    assert!(pos10 < pos20 && pos20 < pos30, "results should appear in order");
}

#[test]
fn test_pipe_mode_continues_after_error() {
    // A failed query in the middle should not abort subsequent queries
    let mut child = Command::new(env!("CARGO_BIN_EXE_fb"))
        .args(&["--core", "-f", "TabSeparatedWithNamesAndTypes"])
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .unwrap();

    let mut stdin = child.stdin.take().unwrap();
    writeln!(stdin, "SELECT 10;").unwrap();
    writeln!(stdin, "SELEC INVALID;").unwrap(); // fails
    writeln!(stdin, "SELECT 30;").unwrap();     // should still run
    drop(stdin);

    let output = child.wait_with_output().unwrap();
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(!output.status.success(), "exit code should be non-zero due to failed query");
    assert!(stdout.contains("10"), "first query result should appear");
    assert!(stdout.contains("30"), "third query result should appear despite middle failure");
}
