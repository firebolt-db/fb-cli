use once_cell::sync::Lazy;
use pest::Parser;
use pest_derive::Parser;
use regex::Regex;
use std::fmt;
use std::time::Instant;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;

/// Extract a human-readable error message from a raw server response body.
///
/// Tries to parse as JSON and look for common error fields; falls back to the
/// trimmed raw text.  Accepts multi-line bodies (pretty-printed JSON objects).
fn readable_error(body: &str) -> String {
    let trimmed = body.trim();
    if let Ok(v) = serde_json::from_str::<serde_json::Value>(trimmed) {
        // Top-level string fields.
        for field in &["error", "message", "description", "detail"] {
            if let Some(s) = v.get(field).and_then(|v| v.as_str()) {
                return s.to_string();
            }
        }
        // `errors` array (e.g. {"errors":[{"description":"..."}]}).
        if let Some(arr) = v.get("errors").and_then(|v| v.as_array()) {
            let msgs: Vec<&str> = arr
                .iter()
                .filter_map(|e| e.get("description").and_then(|d| d.as_str()))
                .collect();
            if !msgs.is_empty() {
                return msgs.join("; ");
            }
        }
        // Valid JSON but no recognised error field — return compact form.
        return serde_json::to_string(&v).unwrap_or_else(|_| trimmed.to_string());
    }
    trimmed.to_string()
}

/// Distinguishes between user-caused and system/infrastructure failures.
/// Values double as process exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Bad SQL, permission denied, query rejected by the engine (HTTP 200 error body or HTTP 400).
    QueryError = 1,
    /// Connection refused, auth failure, HTTP 4xx/5xx other than 400, response decode error.
    SystemError = 2,
}

/// Error returned by [`query()`] that carries an [`ErrorKind`].
/// The error message has already been printed to stderr before this is returned.
#[derive(Debug)]
pub struct QueryFailed(pub ErrorKind);

impl fmt::Display for QueryFailed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            ErrorKind::QueryError => write!(f, "Query failed"),
            ErrorKind::SystemError => write!(f, "System error"),
        }
    }
}

impl std::error::Error for QueryFailed {}

use crate::args::normalize_extras;
use crate::auth::authenticate_service_account;
use crate::context::Context;
use crate::table_renderer;
use crate::transport;
use crate::utils::spin;

// ── Output helpers ────────────────────────────────────────────────────────────
// These macros route output through the TUI channel when running in TUI mode,
// otherwise fall back to println!/eprintln! as before.

macro_rules! out {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.emit(format!($($arg)*))
    };
}

macro_rules! out_err {
    ($ctx:expr, $($arg:tt)*) => {
        $ctx.emit_err(format!($($arg)*))
    };
}

// Format bytes with appropriate unit (B, KB, MB, GB, TB)
fn format_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];

    if bytes == 0 {
        return "0 B".to_string();
    }

    let bytes_f64 = bytes as f64;
    let unit_index = (bytes_f64.log2() / 10.0).floor() as usize;
    let unit_index = unit_index.min(UNITS.len() - 1);

    let value = bytes_f64 / (1024_f64.powi(unit_index as i32));

    if value >= 100.0 {
        format!("{:.0} {}", value, UNITS[unit_index])
    } else if value >= 10.0 {
        format!("{:.1} {}", value, UNITS[unit_index])
    } else {
        format!("{:.2} {}", value, UNITS[unit_index])
    }
}

// Format number with thousand separators
fn format_number(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::new();
    let mut count = 0;

    for c in s.chars().rev() {
        if count > 0 && count % 3 == 0 {
            result.push(',');
        }
        result.push(c);
        count += 1;
    }

    result.chars().rev().collect()
}

const INTERACTIVE_MAX_ROWS: usize = 10_000;
const INTERACTIVE_MAX_BYTES: usize = 1_048_576; // 1 MB


/// Handle client-side dot commands: `.format = value`, `.completion = on|off`, etc.
/// Returns `true` if the input was a dot command (even if the key was unknown),
/// `false` if the line does not start with `.`.
pub fn dot_command(context: &mut Context, line: &str) -> bool {
    static DOT_RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^\.(\w+)\s*(?:=\s*(.*?))?\s*;?\s*$").unwrap());

    let line = line.trim();
    if !line.starts_with('.') {
        return false;
    }

    let caps = match DOT_RE.captures(line) {
        Some(c) => c,
        None => {
            out_err!(context, "Error: invalid dot command: {}", line);
            return true;
        }
    };

    let key = caps.get(1).map_or("", |m| m.as_str());
    let has_eq = line.contains('=');
    let value = caps.get(2).map_or("", |m| m.as_str().trim());

    match key {
        "format" => {
            if !has_eq {
                out_err!(context, "format = {}", context.args.format);
            } else if value.is_empty() {
                context.args.format = "client:auto".to_string();
                context.update_url();
            } else if !value.starts_with("client:") {
                out_err!(
                    context,
                    "Error: .format only accepts client-side formats: client:auto, client:vertical, client:horizontal. \
                     Use --format or 'set output_format=<value>;' for server-side formats."
                );
            } else {
                context.args.format = value.to_string();
                context.update_url();
            }
        }
        "completion" => {
            if !has_eq {
                let status = if context.args.no_completion { "off" } else { "on" };
                out_err!(context, "completion = {}", status);
            } else if value.is_empty() {
                context.args.no_completion = false; // reset to default (enabled)
            } else {
                let val_lower = value.to_lowercase();
                if val_lower == "on" || val_lower == "true" || val_lower == "1" {
                    context.args.no_completion = false;
                } else if val_lower == "off" || val_lower == "false" || val_lower == "0" {
                    context.args.no_completion = true;
                } else {
                    out_err!(context, "Error: invalid value for .completion: '{}'. Use 'on' or 'off'.", value);
                }
            }
        }
        _ => {
            out_err!(context, "Error: unknown client setting '.{}'. Available: .format, .completion", key);
        }
    }
    true
}

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
        if key == "database" {
            context.args.database = String::from(value);
        }

        let mut buf: Vec<String> = vec![];
        buf.push(format!("{key}={value}"));
        buf = normalize_extras(buf, true)?;
        context.args.extra.append(&mut buf);
        buf.append(&mut context.args.extra);
        context.args.extra = normalize_extras(buf, false)?;

        if context.is_tui() && key == "engine" && value == "system" {
            out_err!(context, "\nTo query SYSTEM engine please run 'unset engine'\n");
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
            context.args.format = String::from("client:auto");
        } else if key == "database" {
            context.args.database = String::from("");
        }

        context.update_url();

        return Ok(true);
    }

    Ok(false)
}

/// Apply a `Firebolt-Update-Parameters` header value.
///
/// The header carries a comma-separated list of `key=value` pairs, e.g.:
///   `transaction_id=abc123,transaction_sequence_id=1`
///
/// Each pair is forwarded to `set_args` so it ends up in the URL.
fn apply_update_parameters(context: &mut Context, header_value: &str) -> Result<(), Box<dyn std::error::Error>> {
    for pair in header_value.split(',') {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        set_args(context, &format!("set {}", pair))?;
    }
    Ok(())
}

/// Apply a `Firebolt-Remove-Parameters` or `Firebolt-Reset-Session` header.
///
/// `keys` is a comma-separated list of parameter names to remove from
/// `context.args.extra`, e.g. `"transaction_id,transaction_sequence_id"`.
fn remove_parameters(context: &mut Context, keys: &str) {
    for key in keys.split(',') {
        let key = key.trim();
        if key.is_empty() {
            continue;
        }
        let prefix = format!("{}=", key);
        context.args.extra.retain(|e| !e.starts_with(&prefix));
    }
    context.update_url();
}

// Execute a query silently and return the response body (for internal use like schema queries)
pub async fn query_silent(context: &mut Context, query_text: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let auth = if let Some(sa_token) = &context.sa_token {
        Some(format!("Bearer {}", sa_token.token))
    } else if !context.args.jwt.is_empty() {
        Some(format!("Bearer {}", context.args.jwt))
    } else {
        None
    };
    let unix_socket = (!context.args.unix_socket.is_empty()).then_some(context.args.unix_socket.as_str());
    let response = transport::post(&context.url, unix_socket, query_text.to_string(), auth, true).await?;
    Ok(response.text().await?)
}

/// Send `SELECT 1;` with the current context URL to validate that all query
/// parameters are accepted by the server.  Returns an error message on HTTP
/// 4xx/5xx or connection failure.
pub async fn validate_setting(context: &mut Context) -> Result<(), String> {
    let auth = if let Some(sa_token) = &context.sa_token {
        Some(format!("Bearer {}", sa_token.token))
    } else if !context.args.jwt.is_empty() {
        Some(format!("Bearer {}", context.args.jwt))
    } else {
        None
    };
    let unix_socket = (!context.args.unix_socket.is_empty()).then_some(context.args.unix_socket.as_str());
    let response = transport::post(&context.url, unix_socket, "SELECT 1;".to_string(), auth, true)
        .await
        .map_err(|e| e.to_string())?;
    if !response.status().is_success() {
        let body = response.text().await.unwrap_or_default();
        return Err(readable_error(&body));
    }
    Ok(())
}

/// Render a result table to a String using the display mode from context.
/// Render a result table as a plain-text String using the same renderer as the
/// TUI, so headless output matches the interactive REPL visually.
fn render_table_plain(
    context: &Context,
    columns: &[table_renderer::ResultColumn],
    rows: &[Vec<serde_json::Value>],
    terminal_width: u16,
    max_cell_length: usize,
) -> String {
    let tw = terminal_width.saturating_sub(1);
    let lines = if context.args.is_vertical_display() {
        table_renderer::render_vertical_table_to_tui_lines(columns, rows, tw, max_cell_length)
    } else if context.args.is_horizontal_display() {
        table_renderer::render_horizontal_forced_tui_lines(columns, rows, tw, max_cell_length)
    } else {
        table_renderer::render_table_to_tui_lines(columns, rows, tw, max_cell_length)
    };
    lines
        .into_iter()
        .map(|line| line.0.into_iter().map(|s| s.text).collect::<String>())
        .collect::<Vec<_>>()
        .join("\n")
}

/// In TUI mode, emit a result table as pre-styled TuiLines (no ANSI round-trip).
fn emit_table_tui(
    context: &Context,
    columns: &[table_renderer::ResultColumn],
    rows: &[Vec<serde_json::Value>],
    terminal_width: u16,
    max_cell_length: usize,
) {
    // Leave a 1-column margin so the rightmost border isn't clipped by ratatui.
    let tw = terminal_width.saturating_sub(1);
    let lines = if context.args.is_vertical_display() {
        table_renderer::render_vertical_table_to_tui_lines(columns, rows, tw, max_cell_length)
    } else if context.args.is_horizontal_display() {
        table_renderer::render_horizontal_forced_tui_lines(columns, rows, tw, max_cell_length)
    } else {
        table_renderer::render_table_to_tui_lines(columns, rows, tw, max_cell_length)
    };
    context.emit_styled_lines(lines);
}

/// Extract a human-readable scan-statistics string from the FINISH_SUCCESSFULLY payload.
fn compute_stats(statistics: &Option<serde_json::Value>) -> Option<String> {
    if statistics.is_none() {
        return None;
    }
    statistics.as_ref().and_then(|stats| {
        stats.as_object().map(|obj| {
            let scanned_cache = obj.get("scanned_bytes_cache").and_then(|v| v.as_u64()).unwrap_or(0);
            let scanned_storage = obj.get("scanned_bytes_storage").and_then(|v| v.as_u64()).unwrap_or(0);
            let rows_read = obj.get("rows_read").and_then(|v| v.as_u64()).unwrap_or(0);
            let total_scanned = scanned_cache + scanned_storage;
            if rows_read > 0 || total_scanned > 0 {
                Some(format!(
                    "Scanned: {} rows, {} ({} local, {} remote)",
                    format_number(rows_read),
                    format_bytes(total_scanned),
                    format_bytes(scanned_cache),
                    format_bytes(scanned_storage),
                ))
            } else {
                None
            }
        }).flatten()
    })
}

// Send query and print result.
pub async fn query(context: &mut Context, query_text: String) -> Result<(), Box<dyn std::error::Error>> {
    // Handle set/unset commands
    if set_args(context, &query_text)? {
        return Ok(());
    }

    if unset_args(context, &query_text)? {
        return Ok(());
    }

    if !context.args.sa_id.is_empty() || !context.args.sa_secret.is_empty() {
        authenticate_service_account(context).await?;
    }

    if context.args.verbose {
        out_err!(context, "URL: {}", context.url);
        out_err!(context, "QUERY: {}", query_text);
    }

    let start = Instant::now();

    // Clone query_text for tracking later
    let query_text_for_tracking = query_text.clone();

    let auth = if let Some(sa_token) = &context.sa_token {
        Some(format!("Bearer {}", sa_token.token))
    } else if !context.args.jwt.is_empty() {
        Some(format!("Bearer {}", context.args.jwt))
    } else {
        None
    };
    let unix_socket = (!context.args.unix_socket.is_empty()).then_some(context.args.unix_socket.as_str());
    let url = context.url.clone();
    let async_resp = transport::post(&url, unix_socket, query_text, auth, false);

    // Build a cancel future: prefer the TUI's CancellationToken, fall back to SIGINT.
    let cancel = context.query_cancel.clone();

    // Show spinner in non-interactive mode only for client-side formats.
    let spin_token = CancellationToken::new();
    let mut maybe_spin = if !context.is_tui() && context.args.should_render_table() {
        let t = spin_token.clone();
        Some(task::spawn(async move { spin(t).await; }))
    } else {
        None
    };

    let mut error_kind: Option<ErrorKind> = None;

    select! {
        _ = async {
            if let Some(token) = cancel {
                token.cancelled().await;
            } else {
                let _ = signal::ctrl_c().await;
            }
        } => {
            out_err!(context, "^C");
            error_kind = Some(ErrorKind::SystemError);
        }
        response = async_resp => {
            // Erase the spinner before producing any output, so the backspace
            // sequence lands on the spinner character and not on the table.
            spin_token.cancel();
            if let Some(s) = maybe_spin.take() { let _ = s.await; }

            let elapsed = start.elapsed();

            let mut maybe_request_id: Option<String> = None;
            match response {
                Ok(mut resp) => {
                    let mut updated_url = false;
                    for (header, value) in resp.headers() {
                        if header == "firebolt-update-parameters" {
                            apply_update_parameters(context, value.to_str()?)?;
                            updated_url = true;
                        } else if header == "firebolt-remove-parameters" {
                            remove_parameters(context, value.to_str()?);
                            updated_url = true;
                        } else if header == "firebolt-reset-session" {
                            // End of transaction: clear transaction_id and transaction_sequence_id.
                            remove_parameters(context, "transaction_id,transaction_sequence_id");
                            updated_url = true;
                        } else if header == "X-REQUEST-ID" {
                            maybe_request_id = value.to_str().map_or(None, |l| Some(String::from(l)));
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
                    if updated_url {
                        if context.args.verbose && !context.args.hide_pii {
                            out_err!(context, "URL: {}", context.url);
                        }
                        // Propagate the new extras back to the TUI's own context
                        // (the task runs on a clone; without this the transaction
                        // badge and similar features would never see the changes).
                        if let Some(tx) = &context.tui_output_tx {
                            let _ = tx.send(crate::tui_msg::TuiMsg::ParamUpdate(
                                context.args.extra.clone(),
                            ));
                        }
                    }

                    let status = resp.status();

                    if context.args.should_render_table() && context.is_interactive {
                        // ── Streaming path: emit display rows at the limit,
                        //    keep collecting all rows for csvlens. ────────────
                        use table_renderer::{ErrorDetail, JsonLineMessage, ParsedResult, ResultColumn};

                        let mut columns: Vec<ResultColumn> = Vec::new();
                        let mut display_rows: Vec<Vec<serde_json::Value>> = Vec::new();
                        let mut all_rows: Vec<Vec<serde_json::Value>> = Vec::new();
                        let mut statistics: Option<serde_json::Value> = None;
                        let mut errors: Option<Vec<ErrorDetail>> = None;
                        let mut display_emitted = false;
                        let mut display_byte_count = 0usize;

                        let terminal_width = terminal_size::terminal_size()
                            .map(|(terminal_size::Width(w), _)| w)
                            .unwrap_or(80);

                        let mut line_buf = String::new();
                        let mut stream_err: Option<String> = None;

                        'stream: loop {
                            match resp.chunk().await {
                                Err(e) => { stream_err = Some(e.to_string()); break 'stream; }
                                Ok(None) => break 'stream,
                                Ok(Some(chunk)) => {
                                    match std::str::from_utf8(&chunk) {
                                        Err(_) => { stream_err = Some("Invalid UTF-8 in response".into()); break 'stream; }
                                        Ok(s) => line_buf.push_str(s),
                                    }
                                    while let Some(nl) = line_buf.find('\n') {
                                        let line = line_buf[..nl].trim().to_string();
                                        line_buf.drain(..nl + 1);
                                        if line.is_empty() { continue; }

                                        match serde_json::from_str::<JsonLineMessage>(&line) {
                                            Err(parse_err) => {
                                                // The line isn't a JSONLines message — the server
                                                // likely sent a multi-line JSON error object.
                                                // Drain the rest of the response so we have the
                                                // complete body, then surface a readable message.
                                                let mut body = line.clone();
                                                body.push('\n');
                                                body.push_str(&line_buf);
                                                'drain: loop {
                                                    match resp.chunk().await {
                                                        Ok(Some(c)) => {
                                                            if let Ok(s) = std::str::from_utf8(&c) {
                                                                body.push_str(s);
                                                            }
                                                        }
                                                        _ => break 'drain,
                                                    }
                                                }
                                                let msg = readable_error(&body);
                                                stream_err = Some(if context.args.verbose {
                                                    format!("{} (parse error: {})", msg, parse_err)
                                                } else {
                                                    msg
                                                });
                                                break 'stream;
                                            }
                                            Ok(msg) => match msg {
                                                JsonLineMessage::Start { result_columns, .. } => {
                                                    columns = result_columns;
                                                }
                                                JsonLineMessage::Data { data } => {
                                                    for row in data {
                                                        if !display_emitted {
                                                            let row_bytes: usize = row.iter().map(|v| v.to_string().len()).sum();
                                                            let over_rows = display_rows.len() >= INTERACTIVE_MAX_ROWS;
                                                            let over_bytes = display_byte_count + row_bytes > INTERACTIVE_MAX_BYTES;
                                                            if over_rows || over_bytes {
                                                                let max_cell = if columns.len() == 1 { context.args.max_cell_length * 5 } else { context.args.max_cell_length };
                                                                if context.is_tui() {
                                                                    emit_table_tui(context, &columns, &display_rows, terminal_width, max_cell);
                                                                } else {
                                                                    let rendered = render_table_plain(context, &columns, &display_rows, terminal_width, max_cell);
                                                                    out!(context, "{}", rendered);
                                                                }
                                                                out_err!(context, "Showing first {} rows — collecting remainder for Ctrl+V / /view...",
                                                                    format_number(display_rows.len() as u64));
                                                                display_emitted = true;
                                                            } else {
                                                                display_byte_count += row_bytes;
                                                                display_rows.push(row.clone());
                                                            }
                                                        }
                                                        all_rows.push(row);
                                                    }
                                                    // Send live row count to the TUI running pane.
                                                    if context.is_tui() {
                                                        if let Some(tx) = &context.tui_output_tx {
                                                            let _ = tx.send(crate::tui_msg::TuiMsg::Progress(all_rows.len() as u64));
                                                        }
                                                    }
                                                }
                                                JsonLineMessage::FinishSuccessfully { statistics: s } => {
                                                    statistics = s;
                                                }
                                                JsonLineMessage::FinishWithErrors { errors: errs } => {
                                                    errors = Some(errs);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }

                        if let Some(e) = stream_err {
                            out_err!(context, "Error: {}", e);
                            let kind = if status.as_u16() == 400 { ErrorKind::QueryError } else { ErrorKind::SystemError };
                            error_kind = Some(kind);
                        } else if !status.is_success() && errors.is_none() && columns.is_empty() {
                            // Non-2xx with an empty or unparseable body — show the HTTP status.
                            out_err!(context, "Error: server returned HTTP {}", status.as_u16());
                            let kind = if status.as_u16() == 400 { ErrorKind::QueryError } else { ErrorKind::SystemError };
                            error_kind = Some(kind);
                        } else if let Some(errs) = errors {
                            for err in errs {
                                out_err!(context, "Error: {}", err.description);
                            }
                            error_kind = Some(ErrorKind::QueryError);
                        } else {
                            if !columns.is_empty() {
                                if !display_emitted {
                                    // All rows fit within limits — emit now
                                    let max_cell = if columns.len() == 1 { context.args.max_cell_length * 5 } else { context.args.max_cell_length };
                                    if context.is_tui() {
                                        emit_table_tui(context, &columns, &all_rows, terminal_width, max_cell);
                                    } else {
                                        let rendered = render_table_plain(context, &columns, &all_rows, terminal_width, max_cell);
                                        out!(context, "{}", rendered);
                                    }
                                } else {
                                    // Partial display was already emitted; show the final total
                                    out_err!(context, "Showing {} of {} rows (press Ctrl+V or /view to see all).",
                                        format_number(display_rows.len() as u64),
                                        format_number(all_rows.len() as u64));
                                }
                            }
                            // Always emit ParsedResult on success so the TUI can detect
                            // DDL statements (zero columns) and refresh the schema cache.
                            let parsed_result = ParsedResult {
                                columns: columns.clone(),
                                rows: all_rows,
                                statistics: statistics.clone(),
                                errors: None,
                            };
                            context.emit_parsed_result(&parsed_result);
                            context.last_result = Some(parsed_result);
                            context.last_stats = compute_stats(&statistics);
                        }

                    } else {
                        // ── Buffered path (non-interactive or server-rendered) ──
                        let body = resp.text().await.map_err(|e| -> Box<dyn std::error::Error> { e })?;

                        if context.args.should_render_table() {
                            match table_renderer::parse_jsonlines_compact(&body) {
                                Ok(parsed) => {
                                    context.emit_parsed_result(&parsed);
                                    context.last_result = Some(parsed.clone());
                                    if let Some(errors) = parsed.errors {
                                        for error in errors {
                                            out_err!(context, "Error: {}", error.description);
                                        }
                                        error_kind = Some(ErrorKind::QueryError);
                                    } else if !parsed.columns.is_empty() {
                                        let terminal_width = terminal_size::terminal_size()
                                            .map(|(terminal_size::Width(w), _)| w)
                                            .unwrap_or(80);
                                        let max_cell_length = if parsed.columns.len() == 1 {
                                            context.args.max_cell_length * 5
                                        } else {
                                            context.args.max_cell_length
                                        };
                                        if context.is_tui() {
                                            emit_table_tui(context, &parsed.columns, &parsed.rows, terminal_width, max_cell_length);
                                        } else {
                                            let table_output = render_table_plain(context, &parsed.columns, &parsed.rows, terminal_width, max_cell_length);
                                            out!(context, "{}", table_output);
                                        }
                                        context.last_stats = compute_stats(&parsed.statistics);
                                    }
                                }
                                Err(e) => {
                                    if context.args.verbose {
                                        out_err!(context, "Failed to parse table format: {}", e);
                                    }
                                    context.emit_raw(&body);
                                }
                            }
                        } else {
                            context.emit_raw(&body);
                        }

                        if !status.is_success() {
                            let kind = if status.as_u16() == 400 { ErrorKind::QueryError } else { ErrorKind::SystemError };
                            if error_kind != Some(ErrorKind::SystemError) { error_kind = Some(kind); }
                        }
                    }
                }
                Err(error) => {
                    out_err!(context, "Failed to send the request: {}", error);
                    error_kind = Some(ErrorKind::SystemError);
                },
            };

            let elapsed = format!("{:?}", elapsed / 100000 * 100000);
            if context.args.should_render_table() && !context.is_tui() {
                // Client-side format in headless: emit to stdout so stats
                // follow the table (mirrors TUI output-pane behaviour).
                out!(context, "Time: {elapsed}");
                if let Some(stats) = &context.last_stats {
                    out!(context, "{}", stats);
                }
                if let Some(request_id) = maybe_request_id {
                    out!(context, "Request Id: {request_id}");
                }
                out!(context, "");
            } else if context.is_tui() {
                // TUI: send stats to output pane.
                out_err!(context, "Time: {elapsed}");
                if let Some(stats) = &context.last_stats {
                    out_err!(context, "{}", stats);
                }
                context.emit_newline();
            }
        }
    };

    spin_token.cancel();
    if let Some(s) = maybe_spin {
        let _ = s.await;
    }

    if let Some(kind) = error_kind {
        Err(Box::new(QueryFailed(kind)))
    } else {
        // Track successful query for auto-completion prioritization
        if let Some(usage_tracker) = &context.usage_tracker {
            usage_tracker.track_query(&query_text_for_tracking);
        }
        Ok(())
    }
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
    async fn test_query_connection_error() {
        let mut args = get_args().unwrap();
        args.host = "localhost:8123".to_string();
        args.database = "test_db".to_string();
        let mut context = Context::new(args);
        let query_text = "select 42".to_string();

        // Query should fail when server is not available
        let result = query(&mut context, query_text).await;
        assert!(result.is_err());
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

        // Test setting database updates both args.database and args.extra
        let query = "set database = mydb";
        let result = set_args(&mut context, query).unwrap();
        assert!(result);
        assert_eq!(context.args.database, "mydb");
        assert!(context.args.extra.iter().any(|e| e == "database=mydb"));

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
        assert_eq!(context.args.format, "client:auto");

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
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(1), "1.00 B");
        assert_eq!(format_bytes(100), "100 B");
        assert_eq!(format_bytes(1023), "1023 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(10240), "10.0 KB");
        assert_eq!(format_bytes(102400), "100 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1572864), "1.50 MB");
        assert_eq!(format_bytes(10485760), "10.0 MB");
        assert_eq!(format_bytes(104857600), "100 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
        assert_eq!(format_bytes(1099511627776), "1.00 TB");
    }

    #[test]
    fn test_format_number() {
        assert_eq!(format_number(0), "0");
        assert_eq!(format_number(1), "1");
        assert_eq!(format_number(999), "999");
        assert_eq!(format_number(1000), "1,000");
        assert_eq!(format_number(1234), "1,234");
        assert_eq!(format_number(123456), "123,456");
        assert_eq!(format_number(1234567), "1,234,567");
        assert_eq!(format_number(1234567890), "1,234,567,890");
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

    // ── Transaction parameter helpers ─────────────────────────────────────────

    fn make_context() -> Context {
        let args = get_args().unwrap();
        Context::new(args)
    }

    #[test]
    fn test_apply_update_parameters_single_pair() {
        let mut ctx = make_context();
        assert!(!ctx.in_transaction());
        apply_update_parameters(&mut ctx, "transaction_id=abc123").unwrap();
        assert!(ctx.in_transaction());
        assert!(ctx.args.extra.iter().any(|e| e == "transaction_id=abc123"));
    }

    #[test]
    fn test_apply_update_parameters_multiple_pairs() {
        let mut ctx = make_context();
        apply_update_parameters(&mut ctx, "transaction_id=abc123,transaction_sequence_id=1").unwrap();
        assert!(ctx.in_transaction());
        assert!(ctx.args.extra.iter().any(|e| e == "transaction_id=abc123"));
        assert!(ctx.args.extra.iter().any(|e| e == "transaction_sequence_id=1"));
    }

    #[test]
    fn test_apply_update_parameters_empty_string_is_noop() {
        let mut ctx = make_context();
        let before = ctx.args.extra.clone();
        apply_update_parameters(&mut ctx, "").unwrap();
        assert_eq!(ctx.args.extra, before);
        assert!(!ctx.in_transaction());
    }

    #[test]
    fn test_apply_update_parameters_trailing_comma_ignored() {
        let mut ctx = make_context();
        apply_update_parameters(&mut ctx, "transaction_id=x,").unwrap();
        // trailing comma produces an empty token, which is skipped
        assert!(ctx.in_transaction());
        assert_eq!(ctx.args.extra.iter().filter(|e| e.starts_with("transaction_id=")).count(), 1);
    }

    #[test]
    fn test_apply_update_parameters_preserves_existing_extras() {
        let mut args = get_args().unwrap();
        args.extra = vec!["custom_param=hello".to_string()];
        let mut ctx = Context::new(args);
        apply_update_parameters(&mut ctx, "transaction_id=abc").unwrap();
        assert!(ctx.args.extra.iter().any(|e| e == "custom_param=hello"),
            "pre-existing extras must be preserved");
        assert!(ctx.in_transaction());
    }

    #[test]
    fn test_remove_parameters_single_key() {
        let mut ctx = make_context();
        apply_update_parameters(&mut ctx, "transaction_id=abc123,transaction_sequence_id=1").unwrap();
        assert!(ctx.in_transaction());

        remove_parameters(&mut ctx, "transaction_id");
        assert!(!ctx.in_transaction());
        // sequence_id should still be present
        assert!(ctx.args.extra.iter().any(|e| e.starts_with("transaction_sequence_id=")));
    }

    #[test]
    fn test_remove_parameters_multiple_keys() {
        let mut ctx = make_context();
        apply_update_parameters(&mut ctx, "transaction_id=abc123,transaction_sequence_id=1").unwrap();

        remove_parameters(&mut ctx, "transaction_id,transaction_sequence_id");
        assert!(!ctx.in_transaction());
        assert!(!ctx.args.extra.iter().any(|e| e.starts_with("transaction_sequence_id=")));
    }

    #[test]
    fn test_remove_parameters_preserves_unrelated_extras() {
        let mut args = get_args().unwrap();
        args.extra = vec!["other_param=value".to_string()];
        let mut ctx = Context::new(args);
        apply_update_parameters(&mut ctx, "transaction_id=abc123").unwrap();

        remove_parameters(&mut ctx, "transaction_id");
        assert!(!ctx.in_transaction());
        assert!(ctx.args.extra.iter().any(|e| e == "other_param=value"),
            "unrelated extras must survive remove_parameters");
    }

    #[test]
    fn test_remove_parameters_exact_prefix_match() {
        // "transaction_id" removal must not affect "transaction_id_extra=..."
        // because the prefix checked is "transaction_id=" which won't match.
        let mut args = get_args().unwrap();
        args.extra = vec!["transaction_id_extra=value".to_string()];
        let mut ctx = Context::new(args);
        apply_update_parameters(&mut ctx, "transaction_id=abc").unwrap();

        remove_parameters(&mut ctx, "transaction_id");
        assert!(!ctx.in_transaction());
        assert!(ctx.args.extra.iter().any(|e| e == "transaction_id_extra=value"),
            "transaction_id_extra must not be removed when only transaction_id is removed");
    }

    #[test]
    fn test_remove_parameters_empty_string_is_noop() {
        let mut ctx = make_context();
        apply_update_parameters(&mut ctx, "transaction_id=abc").unwrap();
        let before = ctx.args.extra.clone();
        remove_parameters(&mut ctx, "");
        assert_eq!(ctx.args.extra, before);
    }

    #[test]
    fn test_in_transaction_lifecycle() {
        let mut ctx = make_context();

        // 1. Initially no transaction.
        assert!(!ctx.in_transaction());

        // 2. Server sends Firebolt-Update-Parameters after BEGIN.
        apply_update_parameters(&mut ctx, "transaction_id=deadbeef,transaction_sequence_id=0").unwrap();
        assert!(ctx.in_transaction());

        // 3. Sequence counter increments mid-transaction (server-side increment).
        apply_update_parameters(&mut ctx, "transaction_sequence_id=1").unwrap();
        assert!(ctx.in_transaction(), "still in transaction after sequence bump");

        // 4. Server sends Firebolt-Reset-Session after COMMIT/ROLLBACK.
        remove_parameters(&mut ctx, "transaction_id,transaction_sequence_id");
        assert!(!ctx.in_transaction(), "transaction must be cleared after reset");

        // 5. A brand-new transaction can begin.
        apply_update_parameters(&mut ctx, "transaction_id=cafebabe,transaction_sequence_id=0").unwrap();
        assert!(ctx.in_transaction(), "second transaction must register correctly");
    }
}
