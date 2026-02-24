use std::io::IsTerminal;
use std::sync::Arc;

mod args;
mod auth;
mod completion;
mod context;
mod highlight;
mod meta_commands;
mod query;
mod sql_parser;
mod table_renderer;
mod tui;
mod tui_msg;
mod utils;
mod viewer;

use args::get_args;
use auth::maybe_authenticate;
use completion::schema_cache::SchemaCache;
use completion::usage_tracker::UsageTracker;
use context::Context;
use query::{ErrorKind, QueryFailed, dot_command, query};
use tui::TuiApp;

pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const USER_AGENT: &str = concat!("fdb-cli/", env!("CARGO_PKG_VERSION"));
pub const FIREBOLT_PROTOCOL_VERSION: &str = "2.4";

#[tokio::main]
async fn main() {
    std::process::exit(run().await);
}

/// Returns the process exit code:
///   0 — all queries succeeded
///   1 — one or more queries failed (bad SQL, permission denied, HTTP 400)
///   2 — system/infrastructure error (connection refused, auth failure, HTTP 4xx/5xx other than 400)
async fn run() -> i32 {
    let args = match get_args() {
        Ok(a) => a,
        Err(e) => { eprintln!("Error: {}", e); return ErrorKind::SystemError as i32; }
    };

    if args.version {
        println!("fb-cli version {}", CLI_VERSION);
        return 0;
    }

    let mut context = Context::new(args);
    if let Err(e) = maybe_authenticate(&mut context).await {
        eprintln!("Error: {}", e);
        return ErrorKind::SystemError as i32;
    }

    let query_text = if context.args.command.is_empty() {
        context.args.query.join(" ")
    } else {
        format!("{} {}", context.args.command, context.args.query.join(" "))
    };

    // ── Headless mode: query provided on the command line ────────────────────
    if !query_text.is_empty() {
        return match query(&mut context, query_text).await {
            Ok(()) => 0,
            Err(e) => exit_code_for(&e),
        };
    }

    let is_tty = std::io::stdout().is_terminal() && std::io::stdin().is_terminal();
    context.is_interactive = is_tty;

    // ── Non-interactive (pipe/redirect): fall through to headless behaviour ──
    if !is_tty {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        let mut buffer = String::new();
        let mut worst: i32 = 0;

        for line in stdin.lock().lines() {
            let line = match line {
                Ok(l) => l,
                Err(e) => { eprintln!("Error reading stdin: {}", e); return ErrorKind::SystemError as i32; }
            };
            buffer.push_str(&line);

            // Handle quit/exit before appending a newline (mirrors old REPL behaviour)
            if buffer.trim() == "quit" || buffer.trim() == "exit" {
                buffer.clear();
                break;
            }

            buffer.push('\n');

            let queries = query::try_split_queries(&buffer).unwrap_or_default();
            if !queries.is_empty() {
                for q in queries {
                    let dotcheck = q.trim().trim_end_matches(';').trim();
                    if dot_command(&mut context, dotcheck) {
                        // client-side setting applied; nothing to send to server
                    } else {
                        worst = worst.max(run_query(&mut context, q).await);
                    }
                }
                buffer.clear();
            }
        }

        // Try remaining buffer with implicit semicolon (EOF with incomplete query)
        if !buffer.trim().is_empty() {
            let text = format!("{};", buffer.trim());
            if let Some(queries) = query::try_split_queries(&text) {
                for q in queries {
                    let dotcheck = q.trim().trim_end_matches(';').trim();
                    if dot_command(&mut context, dotcheck) {
                        // client-side setting applied
                    } else {
                        worst = worst.max(run_query(&mut context, q).await);
                    }
                }
            }
        }

        return worst;
    }

    // ── Interactive TUI mode ─────────────────────────────────────────────────
    let schema_cache = Arc::new(SchemaCache::new(context.args.completion_cache_ttl));
    let usage_tracker = Arc::new(UsageTracker::new(10));
    context.usage_tracker = Some(usage_tracker);

    let app = TuiApp::new(context, schema_cache);
    match app.run().await {
        Ok(had_error) => if had_error { 1 } else { 0 },
        Err(e) => { eprintln!("Error: {}", e); ErrorKind::SystemError as i32 }
    }
}

/// Run a single query and return its exit code (0, 1, or 2).
async fn run_query(context: &mut Context, q: String) -> i32 {
    match query(context, q).await {
        Ok(()) => 0,
        Err(e) => exit_code_for(&e),
    }
}

/// Map a query error to an exit code.
/// `QueryFailed` errors have their message already printed; others are printed here.
fn exit_code_for(e: &Box<dyn std::error::Error>) -> i32 {
    if let Some(qf) = e.downcast_ref::<QueryFailed>() {
        qf.0 as i32
    } else {
        // Error propagated via `?` (auth failure, network setup, etc.) — print it now.
        eprintln!("Error: {}", e);
        ErrorKind::SystemError as i32
    }
}
