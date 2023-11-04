use rustyline::{config::Configurer, error::ReadlineError, Cmd, DefaultEditor, EventHandler, KeyCode, KeyEvent, Modifiers};

mod args;
mod auth;
mod context;
mod query;
mod utils;

use args::get_args;
use auth::maybe_authenticate;
use context::Context;
use query::{query, try_split_queries};
use utils::history_path;

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

    let mut rl = DefaultEditor::new()?;
    let history_path = history_path()?;
    rl.set_max_history_size(10_000)?;
    if rl.load_history(&history_path).is_err() {
        eprintln!("No previous history");
    } else if context.args.verbose {
        eprintln!("Loaded history from {:?} and set max_history_size = 10'000", history_path)
    }

    rl.bind_sequence(KeyEvent(KeyCode::Char('o'), Modifiers::CTRL), EventHandler::Simple(Cmd::Newline));

    if !context.args.concise {
        eprintln!("Press Ctrl+D to exit.");
    }

    let mut buffer: String = String::new();
    loop {
        let prompt = if !buffer.trim_start().is_empty() { "~> " } else { "=> " };
        let readline = rl.readline(prompt);

        match readline {
            Ok(line) => {
                buffer += line.as_str();
                buffer += "\n";
                if !line.is_empty() {
                    let queries = try_split_queries(&buffer).unwrap_or_default();

                    if !queries.is_empty() {
                        rl.add_history_entry(buffer.trim())?;
                        rl.append_history(&history_path)?;

                        for q in queries {
                            let _ = query(&mut context, q).await;
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
                                let _ = query(&mut context, q).await;
                            }
                        }
                    }
                }
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    if context.args.verbose {
        eprintln!("Saved history to {:?}", history_path)
    }

    Ok(())
}
