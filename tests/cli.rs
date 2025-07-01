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

#[test]
fn test_basic_query() {
    let (success, stdout, _) = run_fb(&["--core", "SELECT 42"]);
    assert!(success);
    assert!(stdout.contains("42"));
}

#[test]
fn test_set_format() {
    // First set format to TSV
    let (success, stdout, _) = run_fb(&["--core", "--concise", "-f", "TabSeparatedWithNamesAndTypes", "SELECT 42;"]);
    assert!(success);
    assert_eq!(stdout, "?column?\nint\n42\n\n");
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
            "--concise",
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
    assert_eq!(lines.next().unwrap(), "?column?");
    lines.next();
    assert_eq!(lines.next().unwrap(), "a=}&");
    lines.next();
    assert_eq!(lines.next().unwrap(), "?column?");
    lines.next();
    assert_eq!(lines.next().unwrap(), "a=}&");
    lines.next();
    assert_eq!(lines.next().unwrap(), "?column?");
    lines.next();
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
        .args(&[
            "--core",
            "--concise",
            "-f",
            "TabSeparatedWithNamesAndTypes",
        ])
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
