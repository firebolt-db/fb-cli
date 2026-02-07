use crate::highlight::SqlHighlighter;
use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::Validator;
use rustyline::Helper;
use std::borrow::Cow;

/// REPL helper that integrates SqlHighlighter with rustyline
pub struct ReplHelper {
    highlighter: SqlHighlighter,
}

impl ReplHelper {
    /// Create a new REPL helper with the given highlighter
    pub fn new(highlighter: SqlHighlighter) -> Self {
        Self { highlighter }
    }
}

// Implement the Helper trait (required)
impl Helper for ReplHelper {}

// Implement empty Completer (no autocompletion for now)
impl Completer for ReplHelper {
    type Candidate = String;
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
