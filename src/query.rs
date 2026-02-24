use once_cell::sync::Lazy;
use pest::Parser;
use pest_derive::Parser;
use regex::Regex;
use std::fmt;
use std::time::Instant;
use tokio::{select, signal, task};
use tokio_util::sync::CancellationToken;

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
use crate::utils::spin;
use crate::FIREBOLT_PROTOCOL_VERSION;
use crate::USER_AGENT;

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

// Limit rows for interactive display, returning a slice and an optional truncation message.
// Bytes are estimated as the sum of JSON string lengths across all cells in a row.
fn apply_output_limits(rows: &[Vec<serde_json::Value>]) -> (&[Vec<serde_json::Value>], Option<String>) {
    let mut byte_count = 0usize;
    for (i, row) in rows.iter().enumerate() {
        if i >= INTERACTIVE_MAX_ROWS {
            return (
                &rows[..i],
                Some(format!(
                    "Showing {} of {} rows (use /view to see all).",
                    format_number(i as u64),
                    format_number(rows.len() as u64),
                )),
            );
        }
        for val in row {
            byte_count += val.to_string().len();
        }
        if byte_count > INTERACTIVE_MAX_BYTES {
            return (
                &rows[..=i],
                Some(format!(
                    "Showing {} of {} rows (use /view to see all).",
                    format_number((i + 1) as u64),
                    format_number(rows.len() as u64),
                )),
            );
        }
    }
    (rows, None)
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
    } else if key == "completion" {
        // Handle completion setting
        let val_lower = value.to_lowercase();
        if val_lower == "on" || val_lower == "true" || val_lower == "1" {
            context.args.no_completion = false;
        } else if val_lower == "off" || val_lower == "false" || val_lower == "0" {
            context.args.no_completion = true;
        } else {
            out_err!(context, "Invalid value for completion: {}. Use 'on' or 'off'.", value);
        }
        return Ok(true);
    } else {
        let mut buf: Vec<String> = vec![];
        buf.push(format!("{key}={value}"));
        buf = normalize_extras(buf, true)?;
        context.args.extra.append(&mut buf);
        buf.append(&mut context.args.extra);
        context.args.extra = normalize_extras(buf, false)?;

        if !context.args.concise && key == "engine" && value == "system" {
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
            context.args.format = String::from("PSQL");
        } else if key == "completion" {
            // Reset completion to default (enabled)
            context.args.no_completion = false;
        } else if key == "database" {
            context.args.database = String::from("");
        }

        context.update_url();

        return Ok(true);
    }

    Ok(false)
}

// Execute a query silently and return the response body (for internal use like schema queries)
pub async fn query_silent(context: &mut Context, query_text: &str) -> Result<String, Box<dyn std::error::Error + Send + Sync>> {
    let request = reqwest::Client::builder()
        .http2_keep_alive_timeout(std::time::Duration::from_secs(3600))
        .http2_keep_alive_interval(Some(std::time::Duration::from_secs(60)))
        .http2_keep_alive_while_idle(false)
        .tcp_keepalive(Some(std::time::Duration::from_secs(60)))
        .build()?
        .post(context.url.clone())
        .header("user-agent", USER_AGENT)
        .header("Firebolt-Protocol-Version", FIREBOLT_PROTOCOL_VERSION)
        .header("Firebolt-Machine-Query", "true")
        .body(query_text.to_string());

    let request = if let Some(sa_token) = &context.sa_token {
        request.header("authorization", format!("Bearer {}", sa_token.token))
    } else if !context.args.jwt.is_empty() {
        request.header("authorization", format!("Bearer {}", context.args.jwt))
    } else {
        request
    };

    let response = request.send().await?;
    let body = response.text().await?;
    Ok(body)
}

/// Render a result table to a String using the display mode from context.
fn render_table_output(
    context: &Context,
    columns: &[table_renderer::ResultColumn],
    rows: &[Vec<serde_json::Value>],
    terminal_width: u16,
    max_cell_length: usize,
) -> String {
    if context.args.is_horizontal_display() {
        table_renderer::render_table(columns, rows, max_cell_length)
    } else if context.args.is_vertical_display() {
        table_renderer::render_table_vertical(columns, rows, terminal_width, max_cell_length)
    } else if context.args.is_auto_display() {
        if table_renderer::should_use_vertical_mode(columns, terminal_width, context.args.min_col_width) {
            table_renderer::render_table_vertical(columns, rows, terminal_width, max_cell_length)
        } else {
            table_renderer::render_table(columns, rows, max_cell_length)
        }
    } else {
        table_renderer::render_table(columns, rows, max_cell_length)
    }
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
fn compute_stats(concise: bool, statistics: &Option<serde_json::Value>) -> Option<String> {
    if concise || statistics.is_none() {
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
        if !context.args.concise && !context.args.hide_pii && !context.is_tui() {
            out_err!(context, "URL: {}", context.url);
        }

        return Ok(());
    }

    if unset_args(context, &query_text)? {
        if !context.args.concise && !context.args.hide_pii && !context.is_tui() {
            out_err!(context, "URL: {}", context.url);
        }

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
    // Skip the spinner when running inside the TUI (it has its own progress indicator).
    let maybe_spin = if context.is_tui() || context.args.no_spinner || context.args.concise {
        None
    } else {
        let token_clone = finish_token.clone();
        Some(task::spawn(async {
            spin(token_clone).await;
        }))
    };

    // Build a cancel future: prefer the TUI's CancellationToken, fall back to SIGINT.
    let cancel = context.query_cancel.clone();

    let mut error_kind: Option<ErrorKind> = None;

    select! {
        _ = async {
            if let Some(token) = cancel {
                token.cancelled().await;
            } else {
                let _ = signal::ctrl_c().await;
            }
        } => {
            finish_token.cancel();
            if let Some(spin) = maybe_spin {
                spin.await?;
            }
            if !context.args.concise {
                out_err!(context, "^C");
            }
            error_kind = Some(ErrorKind::SystemError);
        }
        response = async_resp => {
            let elapsed = start.elapsed();
            finish_token.cancel();
            if let Some(spin) = maybe_spin {
                spin.await?;
            }

            let mut maybe_request_id: Option<String> = None;
            match response {
                Ok(mut resp) => {
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
                    if updated_url && !context.args.concise && !context.args.hide_pii && !context.is_tui() {
                        out_err!(context, "URL: {}", context.url);
                    }

                    let status = resp.status();

                    if context.args.should_render_table() && context.is_interactive {
                        // ── Streaming path: emit display rows at the limit,
                        //    keep collecting all rows for csvlens. ────────────
                        use table_renderer::{ErrorDetail, JsonLineMessage, ParsedResult, ResultColumn};

                        // Non-success responses won't be in JSONLines format.
                        // Collect the body as plain text and surface it directly.
                        if !status.is_success() {
                            let body = resp.text().await.unwrap_or_default();
                            let msg = body.trim();
                            if msg.is_empty() {
                                out_err!(context, "Error: server returned HTTP {}", status.as_u16());
                            } else {
                                out_err!(context, "Error: {}", msg);
                            }
                            let kind = if status.as_u16() == 400 { ErrorKind::QueryError } else { ErrorKind::SystemError };
                            error_kind = Some(kind);
                        } else {

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
                                            Err(e) => {
                                                let detail = if context.args.verbose {
                                                    format!("{} (raw: {:?})", e, line)
                                                } else {
                                                    e.to_string()
                                                };
                                                stream_err = Some(detail);
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
                                                                    let rendered = render_table_output(context, &columns, &display_rows, terminal_width, max_cell);
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
                            if context.args.verbose {
                                out_err!(context, "Failed to read/parse response: {}", e);
                            } else {
                                out_err!(context, "Error: {}", e);
                            }
                            error_kind = Some(ErrorKind::SystemError);
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
                                        let rendered = render_table_output(context, &columns, &all_rows, terminal_width, max_cell);
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
                            context.last_stats = compute_stats(context.args.concise, &statistics);
                        }

                        } // end else (status.is_success())
                    } else {
                        // ── Buffered path (non-interactive or server-rendered) ──
                        let body = resp.text().await?;

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
                                            let table_output = render_table_output(context, &parsed.columns, &parsed.rows, terminal_width, max_cell_length);
                                            out!(context, "{}", table_output);
                                        }
                                        context.last_stats = compute_stats(context.args.concise, &parsed.statistics);
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
                    if context.args.verbose {
                        out_err!(context, "Failed to send the request: {:?}", error);
                    } else {
                        out_err!(context, "Failed to send the request: {}", error.to_string());
                    }
                    error_kind = Some(ErrorKind::SystemError);
                },
            };

            if !context.args.concise {
                let elapsed = format!("{:?}", elapsed / 100000 * 100000);
                out_err!(context, "Time: {elapsed}");
                if let Some(stats) = &context.last_stats {
                    out_err!(context, "{}", stats);
                }
                if let Some(request_id) = maybe_request_id {
                    if !context.is_tui() {
                        out_err!(context, "Request Id: {request_id}");
                    }
                }
                context.emit_newline();
            }
        }
    };

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
        args.concise = true; // suppress output

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
}
