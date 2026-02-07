use crate::completion::SqlCompleter;
use crate::highlight::SqlHighlighter;
use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::{Context, Helper};
use std::borrow::Cow;

/// REPL helper that integrates SqlHighlighter and SqlCompleter with rustyline
pub struct ReplHelper {
    highlighter: SqlHighlighter,
    completer: SqlCompleter,
}

impl ReplHelper {
    /// Create a new REPL helper with the given highlighter and completer
    pub fn new(highlighter: SqlHighlighter, completer: SqlCompleter) -> Self {
        Self {
            highlighter,
            completer,
        }
    }

    /// Get a mutable reference to the completer
    pub fn completer_mut(&mut self) -> &mut SqlCompleter {
        &mut self.completer
    }
}

// Implement the Helper trait (required)
impl Helper for ReplHelper {}

// Delegate completion to SqlCompleter
impl Completer for ReplHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        self.completer.complete(line, pos, ctx)
    }
}

// Implement empty Hinter (no hints for now)
impl Hinter for ReplHelper {
    type Hint = String;
}

// Implement empty Validator (accept all input)
impl Validator for ReplHelper {}

// Delegate highlighting to SqlHighlighter
impl Highlighter for ReplHelper {
    fn highlight<'l>(&self, line: &'l str, pos: usize) -> Cow<'l, str> {
        self.highlighter.highlight(line, pos)
    }

    fn highlight_char(&self, line: &str, pos: usize) -> bool {
        self.highlighter.highlight_char(line, pos)
    }

    fn highlight_prompt<'b, 's: 'b, 'p: 'b>(&'s self, prompt: &'p str, _default: bool) -> Cow<'b, str> {
        // Don't highlight the prompt itself
        Cow::Borrowed(prompt)
    }

    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        // Don't highlight hints
        Cow::Borrowed(hint)
    }
}
