use rustyline::{config::Configurer, error::ReadlineError, Cmd, Editor, EventHandler, KeyCode, KeyEvent, Modifiers};
use std::io::IsTerminal;

mod args;
mod auth;
mod context;
mod highlight;
mod meta_commands;
mod query;
mod repl_helper;
mod table_renderer;
mod utils;
mod viewer;

use args::get_args;
use auth::maybe_authenticate;
use context::Context;
use highlight::SqlHighlighter;
use meta_commands::handle_meta_command;
use query::{query, try_split_queries};
use repl_helper::ReplHelper;
use utils::history_path;
use viewer::open_csvlens_viewer;

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
        format!("{} {}", context.args.command, context.args.query.join(" ")).to_string()
    };

    if !query_text.is_empty() {
        query(&mut context, query_text).await?;
        return Ok(());
    }

    let is_tty = std::io::stdout().is_terminal() && std::io::stdin().is_terminal();
    context.is_interactive = is_tty;

    // Determine if colors should be used
    let should_highlight = is_tty && context.args.should_use_colors();

    // Initialize highlighter with error handling
    let highlighter = SqlHighlighter::new(should_highlight).unwrap_or_else(|e| {
        if context.args.verbose {
            eprintln!("Failed to initialize syntax highlighting: {}", e);
        }
        SqlHighlighter::new(false).unwrap() // Fallback to disabled
    });

    let helper = ReplHelper::new(highlighter);
    let mut rl: Editor<ReplHelper, _> = Editor::new()?;
    rl.set_helper(Some(helper));

    let history_path = history_path()?;
    rl.set_max_history_size(10_000)?;
    if rl.load_history(&history_path).is_err() {
        if is_tty {
            eprintln!("No previous history");
        }
    } else if context.args.verbose {
        eprintln!("Loaded history from {:?} and set max_history_size = 10'000", history_path)
    }

    rl.bind_sequence(KeyEvent(KeyCode::Char('o'), Modifiers::CTRL), EventHandler::Simple(Cmd::Newline));

    // Bind Ctrl-V to trigger viewer via special marker
    // Using Cmd::AcceptLine alone won't work because we need to detect it was Ctrl-V
    // Instead, we'll keep the two-step approach (Ctrl-V + Enter) which is explicit and clear
    rl.bind_sequence(
        KeyEvent(KeyCode::Char('v'), Modifiers::CTRL),
        EventHandler::Simple(Cmd::Insert(1, "\\view".to_string())),
    );

    if is_tty && !context.args.concise {
        eprintln!("Type \\help for available commands or press Ctrl+V then Enter to view last result. Ctrl+D to exit.");
    }
    let mut buffer: String = String::new();
    let mut has_error = false;
    loop {
        let prompt = if !is_tty {
            // No prompt when stdout is not a terminal (e.g., piped)
            ""
        } else if !buffer.trim_start().is_empty() {
            // Continuation prompt (PROMPT2)
            if let Some(custom_prompt) = &context.prompt2 {
                custom_prompt.as_str()
            } else {
                "~> "
            }
        } else if context.args.extra.iter().any(|arg| arg.starts_with("transaction_id=")) {
            // Transaction prompt (PROMPT3)
            if let Some(custom_prompt) = &context.prompt3 {
                custom_prompt.as_str()
            } else {
                "*> "
            }
        } else {
            // Normal prompt (PROMPT1)
            if let Some(custom_prompt) = &context.prompt1 {
                custom_prompt.as_str()
            } else {
                "=> "
            }
        };
        let readline = rl.readline(prompt);

        match readline {
            Ok(line) => {
                // Check for special commands
                let trimmed = line.trim();

                if trimmed == "\\view" {
                    // Open csvlens viewer for last query result
                    if let Err(e) = open_csvlens_viewer(&context) {
                        eprintln!("Failed to open viewer: {}", e);
                    }
                    continue;
                } else if trimmed == "\\help" {
                    // Show help for special commands
                    eprintln!("Special commands:");
                    eprintln!("  \\view       - Open last query result in csvlens viewer");
                    eprintln!("                (requires client format: client:auto, client:vertical, or client:horizontal)");
                    eprintln!("  \\help       - Show this help message");
                    eprintln!();
                    eprintln!("SQL-style commands:");
                    eprintln!("  set format = <value>;   - Change output format");
                    eprintln!("  unset format;           - Reset format to default");
                    eprintln!();
                    eprintln!("Format values:");
                    eprintln!("  Client-side rendering (prefix with 'client:'):");
                    eprintln!("    client:auto       - Smart switching between horizontal/vertical (default in interactive)");
                    eprintln!("    client:horizontal - Force horizontal table layout");
                    eprintln!("    client:vertical   - Force vertical two-column layout");
                    eprintln!();
                    eprintln!("  Server-side rendering (no prefix):");
                    eprintln!("    PSQL              - PostgreSQL-style format (default in non-interactive)");
                    eprintln!("    JSON              - JSON format");
                    eprintln!("    CSV               - CSV format");
                    eprintln!("    TabSeparatedWithNames    - TSV with headers");
                    eprintln!("    JSONLines_Compact        - JSON Lines format");
                    eprintln!();
                    eprintln!("Examples:");
                    eprintln!("  set format = client:vertical;  # Use client-side vertical display");
                    eprintln!("  set format = JSON;             # Use server-side JSON output");
                    eprintln!();
                    eprintln!("Keyboard shortcuts:");
                    eprintln!("  Ctrl+V then Enter - Open last query result in csvlens viewer (inserts \\view)");
                    eprintln!("  Ctrl+O            - Insert newline (for multi-line queries)");
                    eprintln!("  Ctrl+D            - Exit REPL");
                    eprintln!("  Ctrl+C            - Cancel current input");
                    continue;
                }

                buffer += line.as_str();

                if buffer.trim() == "quit" || buffer.trim() == "exit" {
                    break;
                }

                buffer += "\n";
                if !line.is_empty() {
                    // Check if this is a meta-command (backslash command)
                    if line.trim().starts_with('\\') {
                        if let Err(e) = handle_meta_command(&mut context, line.trim()) {
                            eprintln!("Error processing meta-command: {}", e);
                        }
                        buffer.clear();
                        continue;
                    }

                    let queries = try_split_queries(&buffer).unwrap_or_default();

                    if !queries.is_empty() {
                        rl.add_history_entry(buffer.trim())?;
                        rl.append_history(&history_path)?;

                        for q in queries {
                            if query(&mut context, q).await.is_err() {
                                has_error = true;
                            }
                        }

                        buffer.clear();
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                eprintln!("^C");
                buffer.clear();
            }
            Err(ReadlineError::Eof) => {
                if !buffer.trim().is_empty() {
                    buffer += ";";
                    match try_split_queries(&buffer) {
                        None => {}
                        Some(queries) => {
                            for q in queries {
                                rl.add_history_entry(q.trim())?;
                                rl.append_history(&history_path)?;
                                if query(&mut context, q).await.is_err() {
                                    has_error = true;
                                }
                            }
                        }
                    }
                }
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                has_error = true;
                break;
            }
        }
    }

    if context.args.verbose {
        eprintln!("Saved history to {:?}", history_path)
    }

    if has_error {
        Err("One or more queries failed".into())
    } else {
        Ok(())
    }
}
