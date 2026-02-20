use std::io::IsTerminal;
use std::sync::Arc;

mod args;
mod auth;
mod completion;
mod context;
mod highlight;
mod meta_commands;
mod query;
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
use query::query;
use tui::TuiApp;

pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const USER_AGENT: &str = concat!("fdb-cli/", env!("CARGO_PKG_VERSION"));
pub const FIREBOLT_PROTOCOL_VERSION: &str = "2.3";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = get_args()?;

    if args.version {
        println!("fb-cli version {}", CLI_VERSION);
        return Ok(());
    }

    let mut context = Context::new(args);
    maybe_authenticate(&mut context).await?;

    let query_text = if context.args.command.is_empty() {
        context.args.query.join(" ")
    } else {
        format!("{} {}", context.args.command, context.args.query.join(" "))
    };

    // ── Headless mode: query provided on the command line ────────────────────
    if !query_text.is_empty() {
        query(&mut context, query_text).await?;
        return Ok(());
    }

    let is_tty = std::io::stdout().is_terminal() && std::io::stdin().is_terminal();
    context.is_interactive = is_tty;

    // ── Non-interactive (pipe/redirect): fall through to headless behaviour ──
    if !is_tty {
        use std::io::BufRead;
        let stdin = std::io::stdin();
        let mut buffer = String::new();
        let mut has_error = false;

        for line in stdin.lock().lines() {
            let line = line?;
            buffer.push_str(&line);

            // Handle quit/exit before appending a newline (mirrors old REPL behaviour)
            if buffer.trim() == "quit" || buffer.trim() == "exit" {
                buffer.clear(); // don't process as SQL
                break;
            }

            buffer.push('\n');

            let queries = query::try_split_queries(&buffer).unwrap_or_default();
            if !queries.is_empty() {
                for q in queries {
                    if query(&mut context, q).await.is_err() {
                        has_error = true;
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
                    if query(&mut context, q).await.is_err() {
                        has_error = true;
                    }
                }
            }
        }

        return if has_error { Err("One or more queries failed".into()) } else { Ok(()) };
    }

    // ── Interactive TUI mode ─────────────────────────────────────────────────
    let schema_cache = Arc::new(SchemaCache::new(context.args.completion_cache_ttl));
    let usage_tracker = Arc::new(UsageTracker::new(10));
    context.usage_tracker = Some(usage_tracker);

    let app = TuiApp::new(context, schema_cache);
    let had_error = app.run().await?;

    if had_error {
        Err("One or more queries failed".into())
    } else {
        Ok(())
    }
}
