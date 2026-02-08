use rustyline::{config::Configurer, error::ReadlineError, Cmd, DefaultEditor, EventHandler, KeyCode, KeyEvent, Modifiers};
use std::io::IsTerminal;

mod args;
mod auth;
mod context;
mod meta_commands;
mod query;
mod show;
mod utils;

use args::get_args;
use auth::maybe_authenticate;
use context::Context;
use meta_commands::handle_meta_command;
use query::{query, try_split_queries};
use utils::history_path;

pub const CLI_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const USER_AGENT: &str = concat!("fdb-cli/", env!("CARGO_PKG_VERSION"));
pub const FIREBOLT_PROTOCOL_VERSION: &str = "2.3";

fn print_help() {
    println!("fb - Firebolt CLI v{}\n", CLI_VERSION);
    println!("USAGE:");
    println!("    fb [OPTIONS] [QUERY]");
    println!("    fb auth [SUBCOMMAND]");
    println!("    fb use <database|engine> <name>");
    println!("    fb show <databases|engines>");
    println!();
    println!("QUERY EXECUTION:");
    println!("    fb \"SELECT 42\"              Run a single query");
    println!("    fb                           Start interactive REPL");
    println!();
    println!("AUTHENTICATION:");
    println!("    fb auth                      Interactive authentication setup");
    println!("    fb auth check                Show authentication status");
    println!("    fb auth clear                Clear saved credentials");
    println!();
    println!("CONFIGURATION:");
    println!("    fb use database <name>       Set default database");
    println!("    fb use engine <name>         Set default engine (resolves endpoint)");
    println!();
    println!("DISCOVERY:");
    println!("    fb show databases            List all available databases");
    println!("    fb show engines              List all available engines");
    println!();
    println!("OPTIONS:");
    println!("    --database <NAME>            Database name (transient override)");
    println!("    -d <NAME>                    Alias for --database");
    println!("    --host <HOSTNAME>            Hostname (transient override)");
    println!("    --format <FORMAT>            Output format (PSQL, TabSeparatedWithNames, etc.)");
    println!("    --label <LABEL>              Query label for tracking");
    println!("    --sa-id <ID>                 Service Account ID (transient)");
    println!("    --sa-secret <SECRET>         Service Account Secret (transient)");
    println!("    --account-name <NAME>        Account name (transient)");
    println!("    --verbose                    Enable verbose output");
    println!("    --concise                    Suppress time statistics");
    println!("    --no-spinner                 Disable spinner");
    println!("    --version                    Print version");
    println!("    --help                       Show this help message");
    println!();
    println!("EXAMPLES:");
    println!("    # First-time setup");
    println!("    fb auth");
    println!();
    println!("    # Discover available resources");
    println!("    fb show databases");
    println!("    fb show engines");
    println!();
    println!("    # Set defaults");
    println!("    fb use database my_db");
    println!("    fb use engine my_engine");
    println!();
    println!("    # Run queries");
    println!("    fb \"SELECT 42\"");
    println!("    fb -d other_db \"SELECT * FROM table\"  # Override default");
    println!();
    println!("For more information, visit: https://github.com/firebolt-db/fb-cli");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = get_args()?;

    if args.version {
        println!("fb-cli version {}", CLI_VERSION);
        return Ok(());
    }

    // Handle 'help' command
    if args.help || (!args.query.is_empty() && args.query[0] == "help") {
        print_help();
        return Ok(());
    }

    // Handle 'auth' subcommand
    if !args.query.is_empty() && args.query[0] == "auth" {
        // Check for auth subcommands (use positional words instead of flags)
        if args.query.len() > 1 {
            match args.query[1].as_str() {
                "check" | "status" => return auth::show_auth_status(),
                "clear" | "logout" => return auth::clear_auth(),
                _ => {
                    eprintln!("Unknown auth subcommand: {}", args.query[1]);
                    eprintln!("Available: fb auth check, fb auth clear");
                    std::process::exit(1);
                }
            }
        }

        // Interactive mode only
        return auth::interactive_auth_setup().await;
    }

    // Handle 'use' subcommand for setting database/engine
    if !args.query.is_empty() && args.query[0] == "use" {
        if args.query.len() < 3 {
            eprintln!("Usage: fb use database <name>");
            eprintln!("       fb use engine <name>");
            std::process::exit(1);
        }

        match args.query[1].as_str() {
            "database" => {
                let database_name = args.query[2].clone();
                return auth::set_default_database(database_name).await;
            }
            "engine" => {
                let engine_name = args.query[2].clone();
                return auth::set_default_engine(engine_name).await;
            }
            _ => {
                eprintln!("Unknown use target: {}", args.query[1]);
                eprintln!("Available: database, engine");
                std::process::exit(1);
            }
        }
    }

    // Handle 'show' subcommand for listing databases/engines
    if !args.query.is_empty() && args.query[0] == "show" {
        if args.query.len() < 2 {
            eprintln!("Usage: fb show databases");
            eprintln!("       fb show engines");
            std::process::exit(1);
        }

        match args.query[1].as_str() {
            "databases" | "database" => {
                return show::show_databases().await;
            }
            "engines" | "engine" => {
                return show::show_engines().await;
            }
            _ => {
                eprintln!("Unknown show target: {}", args.query[1]);
                eprintln!("Available: databases, engines");
                std::process::exit(1);
            }
        }
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

    let mut rl = DefaultEditor::new()?;
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

    if is_tty {
        eprintln!("Press Ctrl+D to exit.");
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
