use crate::args::{get_url, Args};
use crate::completion::usage_tracker::UsageTracker;
use crate::table_renderer::ParsedResult;
use crate::tui_msg::{TuiLine, TuiMsg};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ServiceAccountToken {
    pub sa_id: String,
    pub sa_secret: String,
    pub token: String,
    pub until: u64,
}

#[derive(Clone)]
pub struct Context {
    pub args: Args,
    pub url: String,
    pub sa_token: Option<ServiceAccountToken>,
    pub prompt1: Option<String>,
    pub prompt2: Option<String>,
    pub prompt3: Option<String>,
    pub last_result: Option<ParsedResult>,
    pub last_stats: Option<String>,
    pub is_interactive: bool,
    pub usage_tracker: Option<Arc<UsageTracker>>,

    /// When running inside the TUI, all output lines are sent here instead of
    /// going to stdout/stderr.  `None` in headless / non-interactive mode.
    pub tui_output_tx: Option<UnboundedSender<TuiMsg>>,

    /// When running inside the TUI, this token can be cancelled by the user
    /// (Ctrl+C) to abort an in-flight query.  Replaces the SIGINT handler.
    pub query_cancel: Option<CancellationToken>,
}

impl Context {
    pub fn new(args: Args) -> Self {
        let url = get_url(&args);
        Self {
            args,
            url,
            sa_token: None,
            prompt1: None,
            prompt2: None,
            prompt3: None,
            last_result: None,
            last_stats: None,
            is_interactive: false,
            usage_tracker: None,
            tui_output_tx: None,
            query_cancel: None,
        }
    }

    pub fn update_url(&mut self) {
        self.url = get_url(&self.args);
    }

    pub fn set_prompt1(&mut self, prompt: String) {
        self.prompt1 = Some(prompt);
    }

    pub fn set_prompt2(&mut self, prompt: String) {
        self.prompt2 = Some(prompt);
    }

    pub fn set_prompt3(&mut self, prompt: String) {
        self.prompt3 = Some(prompt);
    }

    // ── Output helpers ──────────────────────────────────────────────────────

    /// Emit a line of normal output.  In TUI mode: sends to the output channel.
    /// In headless mode: prints to stdout.
    pub fn emit(&self, text: impl AsRef<str>) {
        let text = text.as_ref();
        if let Some(tx) = &self.tui_output_tx {
            let _ = tx.send(TuiMsg::Line(text.to_string()));
        } else {
            println!("{}", text);
        }
    }

    /// Emit a line of error/diagnostic output.  In TUI mode: sends to the
    /// output channel (same pane, different style applied by TuiApp).
    /// In headless mode: prints to stderr.
    pub fn emit_err(&self, text: impl AsRef<str>) {
        let text = text.as_ref();
        if let Some(tx) = &self.tui_output_tx {
            let _ = tx.send(TuiMsg::Line(text.to_string()));
        } else {
            eprintln!("{}", text);
        }
    }

    /// Emit an empty line (equivalent to `println!()` / `eprintln!()`).
    pub fn emit_newline(&self) {
        if let Some(tx) = &self.tui_output_tx {
            let _ = tx.send(TuiMsg::Line(String::new()));
        } else {
            eprintln!();
        }
    }

    /// Emit raw multi-line text.  In TUI mode each `\n` becomes a new output
    /// line.  In headless mode it is printed without an extra trailing newline.
    pub fn emit_raw(&self, text: impl AsRef<str>) {
        let text = text.as_ref();
        if let Some(tx) = &self.tui_output_tx {
            for line in text.split('\n') {
                let _ = tx.send(TuiMsg::Line(line.to_string()));
            }
        } else {
            print!("{}", text);
        }
    }

    /// Emit pre-styled lines (TUI only; no-op in headless mode — callers must
    /// use a String-based fallback for non-TUI output).
    pub fn emit_styled_lines(&self, lines: Vec<TuiLine>) {
        if let Some(tx) = &self.tui_output_tx {
            let _ = tx.send(TuiMsg::StyledLines(lines));
        }
    }

    /// Send the parsed result back to the TUI so it can be used by the csvlens viewer / export.
    /// No-op in headless mode (caller already sets `context.last_result` directly).
    pub fn emit_parsed_result(&self, result: &crate::table_renderer::ParsedResult) {
        if let Some(tx) = &self.tui_output_tx {
            let _ = tx.send(TuiMsg::ParsedResult(result.clone()));
        }
    }

    /// Returns `true` when running inside the TUI event loop.
    pub fn is_tui(&self) -> bool {
        self.tui_output_tx.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_context_creation() {
        let mut args = crate::args::get_args().unwrap();
        args.host = "localhost:8123".to_string();
        args.database = "test_db".to_string();

        let context = Context::new(args);

        assert!(context.url.contains("localhost:8123"));
        assert!(context.url.contains("database=test_db"));
        assert!(context.sa_token.is_none());
        assert!(context.last_result.is_none());
    }
}
