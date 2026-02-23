pub mod completion_popup;
pub mod fuzzy_popup;
pub mod history;
pub mod history_search;
pub mod layout;
pub mod output_pane;
pub mod signature_hint;

use std::sync::Arc;
use std::time::{Duration, Instant};


use crossterm::{
    event::{
        self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyboardEnhancementFlags,
        KeyModifiers, MouseEventKind, PopKeyboardEnhancementFlags, PushKeyboardEnhancementFlags,
    },
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{
    backend::CrosstermBackend,
    layout::{Constraint, Layout, Rect},
    style::{Color, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Paragraph},
    Terminal,
};
use crate::tui_msg::TuiMsg;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tui_textarea::{CursorMove, Input, TextArea};

use crate::completion::context_analyzer::ContextAnalyzer;
use crate::completion::fuzzy_completer::FuzzyCompleter;
use crate::completion::schema_cache::SchemaCache;
use crate::completion::usage_tracker::UsageTracker;
use crate::completion::SqlCompleter;
use crate::context::Context;
use crate::highlight::SqlHighlighter;
use crate::meta_commands::handle_meta_command;
use crate::query::{query, try_split_queries};
use crate::viewer::open_csvlens_viewer;
use crate::CLI_VERSION;

use completion_popup::CompletionState;
use fuzzy_popup::FuzzyState;
use history::History;
use history_search::HistorySearch;
use layout::compute_layout;
use output_pane::OutputPane;

const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

/// Parse `/benchmark [N] <query>` arguments.
/// Returns `(n_runs, query_text)` where `n_runs` defaults to 3.
fn parse_benchmark_args(cmd: &str) -> (usize, String) {
    let rest = cmd
        .strip_prefix("/benchmark")
        .unwrap_or(cmd)
        .trim()
        .to_string();

    // Try to parse an optional numeric run count at the start
    let mut chars = rest.splitn(2, |c: char| c.is_whitespace());
    if let Some(first) = chars.next() {
        if let Ok(n) = first.parse::<usize>() {
            let query = chars.next().unwrap_or("").trim().to_string();
            return (n.max(1), query);
        }
    }

    // No number — entire rest is the query, default 3 runs
    (3, rest)
}

/// Expand a command alias to a full SQL query, returning `None` if the command
/// is not a known alias.
///
/// Adding a new alias: add a branch to the `match` below.
fn expand_command_alias(cmd: &str) -> Option<String> {
    let parts: Vec<&str> = cmd.splitn(3, char::is_whitespace).collect();
    match parts[0] {
        "/qh" => {
            // /qh [limit] [since_minutes]
            let limit = parts.get(1).and_then(|s| s.parse::<u64>().ok()).unwrap_or(10);
            let since_minutes = parts.get(2).and_then(|s| s.parse::<u64>().ok()).unwrap_or(60);
            Some(format!(
                "SELECT * FROM information_schema.engine_user_query_history \
                 WHERE start_time > now() - interval '{since_minutes} minutes' \
                 ORDER BY start_time DESC LIMIT {limit}"
            ))
        }
        _ => None,
    }
}

/// Percent-decode a `key=value` setting string for display.
/// Splits on the first `=` so only the value part is decoded.
fn url_decode_setting(s: &str) -> String {
    if let Some((key, val)) = s.split_once('=') {
        let decoded = urlencoding::decode(val)
            .map(|v| v.into_owned())
            .unwrap_or_else(|_| val.to_string());
        format!("{}={}", key, decoded)
    } else {
        s.to_string()
    }
}

fn format_with_commas(n: u64) -> String {
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, c) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(c);
    }
    result.chars().rev().collect()
}

pub struct TuiApp {
    context: Context,
    textarea: TextArea<'static>,
    output: OutputPane,
    history: History,
    schema_cache: Arc<SchemaCache>,
    completer: SqlCompleter,
    fuzzy_completer: FuzzyCompleter,
    highlighter: SqlHighlighter,

    // Query execution state
    query_rx: Option<mpsc::UnboundedReceiver<TuiMsg>>,
    cancel_token: Option<CancellationToken>,
    is_running: bool,
    query_start: Option<Instant>,
    spinner_tick: u64,
    running_hint: String, // e.g. "Showing first N rows — collecting remainder..."
    progress_rows: u64,   // rows received so far (streamed from query task)
    /// Set when the current query returns a result with zero columns (DDL indicator).
    /// Triggers an async schema-cache refresh on successful completion.
    pending_schema_refresh: bool,

    /// Active tab-completion popup session; `None` when the popup is closed.
    completion_state: Option<CompletionState>,

    /// Active Ctrl+Space fuzzy search session; `None` when closed.
    fuzzy_state: Option<FuzzyState>,

    /// Whether the Ctrl+H help overlay is visible (shown while key is held).
    help_visible: bool,

    /// Active Ctrl+R reverse-search session; `None` when not searching.
    history_search: Option<HistorySearch>,

    /// Current function-signature hint: (func_name, [sig1, sig2, ...]).
    /// `None` when the cursor is not inside a function call.
    signature_hint: Option<(String, Vec<String>)>,

    // After leaving alt-screen for csvlens we need a full redraw
    needs_clear: bool,
    should_quit: bool,
    pub has_error: bool,

    /// Temporary message shown in the status bar, with the time it was set.
    flash_message: Option<(String, Instant)>,

    /// When true, open the csvlens viewer at the top of the next event-loop tick
    /// (outside any event-handler so crossterm's global reader is fully idle).
    pending_viewer: bool,
}

impl TuiApp {
    pub fn new(context: Context, schema_cache: Arc<SchemaCache>) -> Self {
        let usage_tracker = context
            .usage_tracker
            .clone()
            .unwrap_or_else(|| Arc::new(UsageTracker::new(10)));
        let completer = SqlCompleter::new(
            schema_cache.clone(),
            usage_tracker.clone(),
            !context.args.no_completion,
        );

        let history_path = crate::utils::history_path().unwrap_or_default();
        let mut history = History::new(history_path);
        let _ = history.load();

        let textarea = Self::make_textarea();
        let highlighter = SqlHighlighter::new(!context.args.no_completion).unwrap_or_else(|_| {
            SqlHighlighter::new(false).unwrap()
        });
        let fuzzy_completer = FuzzyCompleter::new(schema_cache.clone(), usage_tracker);

        Self {
            context,
            textarea,
            output: OutputPane::new(),
            history,
            schema_cache,
            completer,
            fuzzy_completer,
            highlighter,
            query_rx: None,
            cancel_token: None,
            is_running: false,
            query_start: None,
            spinner_tick: 0,
            running_hint: String::new(),
            progress_rows: 0,
            pending_schema_refresh: false,
            completion_state: None,
            fuzzy_state: None,
            help_visible: false,
            history_search: None,
            signature_hint: None,
            needs_clear: false,
            should_quit: false,
            has_error: false,
            flash_message: None,
            pending_viewer: false,
        }
    }

    fn make_textarea() -> TextArea<'static> {
        let mut ta = TextArea::default();
        // Disable the current-line underline — terminal underlines inherit the
        // foreground colour, which would make syntax-highlighted lines look
        // inconsistent (each token segment would draw its underline in a
        // different colour).
        ta
    }

    // ── Public entry point ───────────────────────────────────────────────────

    pub async fn run(mut self) -> Result<bool, Box<dyn std::error::Error>> {
        enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
        // Enable Kitty keyboard protocol so Shift+Enter is distinguishable from Enter.
        // Silently ignore on terminals that don't support it.
        let _ = execute!(
            stdout,
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        );
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend)?;

        // Kick off background schema cache refresh
        if !self.context.args.no_completion {
            let cache = self.schema_cache.clone();
            let mut ctx_clone = self.context.clone();
            tokio::spawn(async move {
                if let Err(_e) = cache.refresh(&mut ctx_clone).await {
                    // silently ignore – completion just won't work
                }
            });
        }

        let result = self.event_loop(&mut terminal).await;

        disable_raw_mode()?;
        let _ = execute!(terminal.backend_mut(), PopKeyboardEnhancementFlags);
        execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;

        result
    }

    // ── Event loop ───────────────────────────────────────────────────────────

    async fn event_loop(
        &mut self,
        terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    ) -> Result<bool, Box<dyn std::error::Error>> {
        loop {
            // ── Launch csvlens here, before event::poll, so crossterm's global
            //    reader is fully idle (not inside any event handler).
            if self.pending_viewer {
                self.pending_viewer = false;
                self.run_viewer(terminal);
            }

            self.drain_query_output();

            if self.is_running {
                self.spinner_tick += 1;
            }

            if self.needs_clear {
                terminal.clear()?;
                self.needs_clear = false;
            }

            terminal.draw(|f| self.render(f))?;

            // Poll with a short timeout so the spinner animates even without input
            if event::poll(Duration::from_millis(100))? {
                match event::read()? {
                    Event::Key(key) => {
                        if self.handle_key(key).await {
                            break;
                        }
                        self.update_signature_hint();
                    }

                    Event::Mouse(mouse) => match mouse.kind {
                        MouseEventKind::ScrollUp => self.output.scroll_up(8),
                        MouseEventKind::ScrollDown => self.output.scroll_down(8),
                        _ => {} // ignore clicks — Shift+drag still works for terminal selection
                    },
                    Event::Resize(_, _) => {} // redraw on next tick
                    _ => {}
                }
            }

            if self.should_quit {
                break;
            }
        }

        Ok(self.has_error)
    }

    // ── Query output draining ────────────────────────────────────────────────

    fn drain_query_output(&mut self) {
        let rx = match &mut self.query_rx {
            Some(r) => r,
            None => return,
        };

        loop {
            match rx.try_recv() {
                Ok(TuiMsg::Progress(n)) => {
                    self.progress_rows = n;
                }
                Ok(TuiMsg::ParsedResult(result)) => {
                    if result.columns.is_empty() {
                        self.pending_schema_refresh = true;
                    }
                    self.context.last_result = Some(result);
                }
                Ok(TuiMsg::StyledLines(lines)) => {
                    self.output.push_tui_lines(lines);
                }
                Ok(TuiMsg::Line(line)) => {
                    // Capture the "Showing first N rows..." hint for the running pane only.
                    if self.is_running && line.starts_with("Showing first ") {
                        self.running_hint = line;
                        continue;
                    }
                    if line.starts_with("Time: ") || line.starts_with("Scanned: ") {
                        self.output.push_stat(&line);
                    } else if line.starts_with("Error: ") || line.starts_with("^C") {
                        self.output.push_error(&line);
                    } else {
                        self.output.push_ansi_text(&line);
                    }
                }
                Err(mpsc::error::TryRecvError::Empty) => break,
                Err(mpsc::error::TryRecvError::Disconnected) => {
                    // Query task finished
                    self.is_running = false;
                    self.running_hint.clear();
                    self.cancel_token = None;
                    self.query_rx = None;

                    // A zero-column result signals likely DDL — refresh the
                    // schema cache so new/dropped tables appear in completion.
                    // The flag is only set on success (ParsedResult is only
                    // emitted when the query completed without errors).
                    if self.pending_schema_refresh && !self.context.args.no_completion {
                        self.pending_schema_refresh = false;
                        let cache = self.schema_cache.clone();
                        let mut ctx_clone = self.context.clone();
                        tokio::spawn(async move {
                            let _ = cache.refresh(&mut ctx_clone).await;
                        });
                    }

                    break;
                }
            }
        }
    }

    // ── Key handling ─────────────────────────────────────────────────────────

    /// Returns `true` when the app should exit.
    async fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        // ── Ctrl+H help overlay ──────────────────────────────────────────────
        // Handled before everything else so it works during queries too.
        if key.code == KeyCode::Char('h') && key.modifiers.contains(KeyModifiers::CONTROL) {
            self.help_visible = true;
            return false;
        }
        // Escape or q closes the help overlay (and nothing else).
        if self.help_visible
            && (key.code == KeyCode::Esc
                || key.code == KeyCode::Char('q'))
        {
            self.help_visible = false;
            return false;
        }
        // While the overlay is open, swallow all other keys.
        if self.help_visible {
            return false;
        }

        // While a query is running only Ctrl+C is accepted (to cancel)
        if self.is_running {
            if key.code == KeyCode::Char('c') && key.modifiers.contains(KeyModifiers::CONTROL) {
                if let Some(token) = &self.cancel_token {
                    token.cancel();
                }
            }
            return false;
        }

        // ── Ctrl+R history search mode ────────────────────────────────────────
        if self.history_search.is_some() {
            return self.handle_history_search_key(key).await;
        }

        // ── Completion popup navigation ───────────────────────────────────────
        if self.completion_state.is_some() {
            return self.handle_completion_key(key).await;
        }

        // ── Fuzzy search overlay ──────────────────────────────────────────────
        if self.fuzzy_state.is_some() {
            return self.handle_fuzzy_key(key).await;
        }

        match (key.code, key.modifiers) {
            // ── Exit ──────────────────────────────────────────────────────
            (KeyCode::Char('d'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return true;
            }

            // ── Ctrl+R: start reverse history search ─────────────────────
            (KeyCode::Char('r'), m) if m.contains(KeyModifiers::CONTROL) => {
                let saved = self.textarea.lines().join("\n");
                let entries = self.history.entries().to_vec();
                self.history_search = Some(HistorySearch::new(saved, &entries));
                // Immediately preview the most recent match
                self.preview_history_match(&entries);
            }

            // ── Cancel / clear ────────────────────────────────────────────
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.completion_state = None;
                self.reset_textarea();
                self.history.reset_navigation();
            }

            // ── Ctrl+V: open csvlens ──────────────────────────────────────
            (KeyCode::Char('v'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.open_viewer();
            }

            // ── Ctrl+Space: open fuzzy schema search ─────────────────────
            (KeyCode::Char(' '), m) if m.contains(KeyModifiers::CONTROL) => {
                self.open_fuzzy_search();
            }

            // ── Alt+F: format SQL ─────────────────────────────────────────
            (KeyCode::Char('f'), m) if m.contains(KeyModifiers::ALT) => {
                self.format_sql();
            }

            // ── Tab: trigger / navigate completion ───────────────────────
            (KeyCode::Tab, _) => {
                self.trigger_or_advance_completion();
            }

            // ── Shift+Tab: navigate completion backwards ──────────────────
            (KeyCode::BackTab, _) => {
                if let Some(cs) = &mut self.completion_state {
                    cs.prev();
                }
            }

            // ── Shift+Enter: always insert newline ───────────────────────
            (KeyCode::Enter, m) if m.contains(KeyModifiers::SHIFT) => {
                self.textarea.insert_newline();
            }

            // ── Submit on Enter ───────────────────────────────────────────
            (KeyCode::Enter, m) if !m.contains(KeyModifiers::ALT) => {
                let text = self.textarea.lines().join("\n");
                let trimmed = text.trim().to_string();

                if trimmed.is_empty() {
                    return false;
                }

                // Slash meta-commands
                if trimmed.starts_with('/') {
                    self.handle_slash_command(&trimmed).await;
                    self.reset_textarea();
                    return false;
                }

                if trimmed == "quit" || trimmed == "exit" {
                    self.should_quit = true;
                    return true;
                }

                if let Some(queries) = try_split_queries(&trimmed) {
                    self.history.add(trimmed.clone());
                    self.history.reset_navigation();
                    self.execute_queries(trimmed, queries).await;
                    self.reset_textarea();
                } else {
                    // Query not complete yet – add newline
                    self.textarea.insert_newline();
                }
            }

            // ── History: Up/Down at buffer boundaries; Ctrl+Up/Down always ──
            (KeyCode::Up, m) if m.contains(KeyModifiers::CONTROL) => {
                let current = self.textarea.lines().join("\n");
                if let Some(entry) = self.history.go_back(&current) {
                    self.set_textarea_content(&entry);
                }
            }
            (KeyCode::Down, m) if m.contains(KeyModifiers::CONTROL) => {
                if let Some(entry) = self.history.go_forward() {
                    self.set_textarea_content(&entry);
                }
            }
            (KeyCode::Up, _) => {
                let (row, _) = self.textarea.cursor();
                if row == 0 {
                    let current = self.textarea.lines().join("\n");
                    if let Some(entry) = self.history.go_back(&current) {
                        self.set_textarea_content(&entry);
                    }
                } else {
                    // Move cursor within the buffer; keep history navigation active so
                    // pressing Up again after reaching row 0 continues going back.
                    self.textarea.input(Input::from(key));
                }
            }
            (KeyCode::Down, _) => {
                let (row, _) = self.textarea.cursor();
                let last_row = self.textarea.lines().len().saturating_sub(1);
                if row >= last_row {
                    if let Some(entry) = self.history.go_forward() {
                        self.set_textarea_content(&entry);
                    }
                } else {
                    // Move cursor within the buffer; keep history navigation active.
                    self.textarea.input(Input::from(key));
                }
            }

            // ── Scroll output pane ────────────────────────────────────────
            (KeyCode::PageUp, _) => {
                self.output.scroll_up(10);
            }
            (KeyCode::PageDown, _) => {
                self.output.scroll_down(10);
            }

            // ── All other keys → textarea ─────────────────────────────────
            _ => {
                let input = Input::from(key);
                self.textarea.input(input);
                self.history.reset_navigation();
            }
        }

        false
    }

    // ── Ctrl+R history search ────────────────────────────────────────────────

    /// Key handler active while `history_search` is `Some`.
    /// Returns `true` if the app should quit (never, but matches signature).
    async fn handle_history_search_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        let entries = self.history.entries().to_vec();

        // Compute the visible list height for scroll tracking (approximation)
        let list_h = history_search::MAX_VISIBLE;

        match (key.code, key.modifiers) {
            // Ctrl+R or Up arrow: cycle to next older match
            (KeyCode::Char('r'), m) if m.contains(KeyModifiers::CONTROL) => {
                if let Some(hs) = &mut self.history_search {
                    hs.search_older(&entries, list_h);
                }
                self.preview_history_match(&entries);
            }
            (KeyCode::Up, _) => {
                if let Some(hs) = &mut self.history_search {
                    hs.select_older(list_h);
                }
                self.preview_history_match(&entries);
            }

            // Down arrow: cycle to next newer match
            (KeyCode::Down, _) => {
                if let Some(hs) = &mut self.history_search {
                    hs.select_newer(list_h);
                }
                self.preview_history_match(&entries);
            }

            // Escape or Ctrl+G: cancel, restore saved content
            (KeyCode::Esc, _)
            | (KeyCode::Char('g'), crossterm::event::KeyModifiers::CONTROL) => {
                let saved = self
                    .history_search
                    .take()
                    .map(|hs| hs.saved_content().to_string())
                    .unwrap_or_default();
                self.set_textarea_content(&saved);
            }

            // Ctrl+C: cancel and clear
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.history_search = None;
                self.reset_textarea();
            }

            // Enter: accept — textarea already shows the preview
            (KeyCode::Enter, _) => {
                self.history_search = None;
                // textarea already has the previewed content; nothing else to do
            }

            // Backspace: remove one character from the query
            (KeyCode::Backspace, _) => {
                if let Some(hs) = &mut self.history_search {
                    hs.pop_char(&entries);
                }
                self.preview_history_match(&entries);
            }

            // Printable character: append to search query
            (KeyCode::Char(c), m) if !m.contains(KeyModifiers::CONTROL) => {
                if let Some(hs) = &mut self.history_search {
                    hs.push_char(c, &entries);
                }
                self.preview_history_match(&entries);
            }

            // Any other key: accept match, exit search, re-process key normally
            _ => {
                self.history_search = None;
                // textarea already has the previewed content
                return Box::pin(self.handle_key(key)).await;
            }
        }

        false
    }

    /// Update the textarea to show a preview of the currently selected history match.
    /// On no match, restores the saved content.
    fn preview_history_match(&mut self, entries: &[String]) {
        let content = self
            .history_search
            .as_ref()
            .and_then(|hs| hs.matched(entries))
            .map(|s| s.to_string())
            .or_else(|| {
                self.history_search
                    .as_ref()
                    .map(|hs| hs.saved_content().to_string())
            })
            .unwrap_or_default();
        self.set_textarea_content(&content);
    }

    // ── Signature hint ───────────────────────────────────────────────────────

    /// Recompute which function (if any) the cursor is currently inside and
    /// update `self.signature_hint` accordingly.
    fn update_signature_hint(&mut self) {
        // Don't show during overlays or while a query is running
        if self.is_running
            || self.history_search.is_some()
            || self.fuzzy_state.is_some()
            || self.help_visible
            || self.context.args.no_completion
        {
            self.signature_hint = None;
            return;
        }

        let lines = self.textarea.lines();
        let (cursor_row, cursor_col) = self.textarea.cursor();
        // Compute cursor byte offset in the joined SQL
        let byte_offset: usize = lines[..cursor_row]
            .iter()
            .map(|l| l.len() + 1) // +1 for \n
            .sum::<usize>()
            + cursor_col;
        let full_sql = lines.join("\n");

        match signature_hint::detect_function_at_cursor(&full_sql, byte_offset) {
            Some(func_name) => {
                let sigs = self.schema_cache.get_signatures(&func_name);
                self.signature_hint = Some((func_name, sigs));
            }
            None => {
                self.signature_hint = None;
            }
        }
    }

    // ── Tab completion ───────────────────────────────────────────────────────

    /// Called on Tab when no popup is open: computes candidates and opens the popup.
    /// If there is exactly one candidate, accepts it immediately.
    /// If the popup is already open, advances to the next item.
    fn trigger_or_advance_completion(&mut self) {
        if let Some(cs) = &mut self.completion_state {
            cs.next();
            return;
        }

        // Compute current cursor position in terms of the full content
        let (cursor_row, cursor_col) = self.textarea.cursor();
        let lines = self.textarea.lines();
        // byte offset of cursor in the full joined text
        let byte_offset: usize = lines[..cursor_row].iter().map(|l| l.len() + 1).sum::<usize>()
            + cursor_col;
        // current line up to cursor for completion
        let line_to_cursor = &lines[cursor_row][..cursor_col];
        let full_sql = lines.join("\n");

        let (word_start_byte_in_line, mut items) =
            self.completer.complete_at(line_to_cursor, cursor_col, &full_sql);

        if items.is_empty() {
            return;
        }

        // If the character immediately after the cursor is already `(`, strip the
        // trailing `(` from function completion inserts so we don't double up.
        let next_char_is_paren = lines
            .get(cursor_row)
            .and_then(|line| line[cursor_col..].chars().next())
            .map(|c| c == '(')
            .unwrap_or(false);
        if next_char_is_paren {
            for item in &mut items {
                if item.value.ends_with('(') {
                    item.value.pop();
                }
            }
        }

        // word_start as byte offset in the FULL text (for deletion later)
        let word_start_byte =
            byte_offset - (cursor_col - word_start_byte_in_line);

        // For single-item completions, accept immediately
        if items.len() == 1 {
            let value = items.into_iter().next().unwrap().value;
            let partial_len = cursor_col - word_start_byte_in_line;
            // Jump to word start, then delete the partial word forward, then insert.
            // (delete_str deletes FORWARD from cursor, so we must reposition first.)
            self.textarea.move_cursor(CursorMove::Jump(cursor_row as u16, word_start_byte_in_line as u16));
            self.textarea.delete_str(partial_len);
            self.textarea.insert_str(&value);
            return;
        }

        let cs = CompletionState::new(items, word_start_byte, word_start_byte_in_line, cursor_row);
        self.completion_state = Some(cs);
    }

    /// Accept the currently selected completion item, replace the partial word, close popup.
    fn accept_completion(&mut self) {
        let cs = match self.completion_state.take() {
            Some(c) => c,
            None => return,
        };

        let selected = match cs.selected_item() {
            Some(item) => item.value.clone(),
            None => return,
        };

        let (cursor_row, cursor_col) = self.textarea.cursor();
        let partial_len = cursor_col - cs.word_start_col;
        // Jump to word start, then delete the partial word forward, then insert.
        // (delete_str deletes FORWARD from cursor, so we must reposition first.)
        self.textarea.move_cursor(CursorMove::Jump(cursor_row as u16, cs.word_start_col as u16));
        self.textarea.delete_str(partial_len);
        self.textarea.insert_str(&selected);
    }

    /// Key handler active while the completion popup is open.
    /// Returns `true` only if the app should quit (never, but matches signature).
    async fn handle_completion_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        match (key.code, key.modifiers) {
            // Tab / Down: next item
            (KeyCode::Tab, _) | (KeyCode::Down, _) => {
                if let Some(cs) = &mut self.completion_state {
                    cs.next();
                }
            }
            // Shift+Tab / Up: previous item
            (KeyCode::BackTab, _) | (KeyCode::Up, _) => {
                if let Some(cs) = &mut self.completion_state {
                    cs.prev();
                }
            }
            // Enter: accept selection (do NOT submit query)
            (KeyCode::Enter, _) => {
                self.accept_completion();
            }
            // Escape: close without accepting
            (KeyCode::Esc, _) => {
                self.completion_state = None;
            }
            // Any other key: close popup and re-dispatch normally
            _ => {
                self.completion_state = None;
                return Box::pin(self.handle_key(key)).await;
            }
        }
        false
    }

    // ── Fuzzy search ─────────────────────────────────────────────────────────

    /// Extract the tables referenced in the current textarea content so fuzzy
    /// search can apply the same context-aware priority as tab completion.
    fn current_sql_tables(&self) -> Vec<String> {
        let lines = self.textarea.lines();
        let sql = lines.join("\n");
        ContextAnalyzer::extract_tables(&sql)
    }

    fn open_fuzzy_search(&mut self) {
        let tables = self.current_sql_tables();
        let mut state = FuzzyState::new();
        state.items = self.fuzzy_completer.search("", 100, &tables);
        self.fuzzy_state = Some(state);
    }

    /// Key handler for the fuzzy popup. Returns true if app should quit.
    async fn handle_fuzzy_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
        match (key.code, key.modifiers) {
            // Escape or Ctrl+Space: close
            (KeyCode::Esc, _)
            | (KeyCode::Char(' '), crossterm::event::KeyModifiers::CONTROL) => {
                self.fuzzy_state = None;
            }

            // Enter: accept selected item
            (KeyCode::Enter, _) => {
                if let Some(fs) = &self.fuzzy_state {
                    if let Some(item) = fs.selected_item() {
                        let value = item.insert_value.clone();
                        self.fuzzy_state = None;
                        self.textarea.insert_str(&value);
                    } else {
                        self.fuzzy_state = None;
                    }
                }
            }

            // Down / Tab: next item
            (KeyCode::Down, _) | (KeyCode::Tab, _) => {
                if let Some(fs) = &mut self.fuzzy_state {
                    fs.next();
                }
            }

            // Up / Shift+Tab: previous item
            (KeyCode::Up, _) | (KeyCode::BackTab, _) => {
                if let Some(fs) = &mut self.fuzzy_state {
                    fs.prev();
                }
            }

            // Backspace: delete last char from query and re-search
            (KeyCode::Backspace, _) => {
                if let Some(fs) = &mut self.fuzzy_state {
                    fs.pop_char();
                    let q = fs.query.clone();
                    let tables = self.current_sql_tables();
                    let items = self.fuzzy_completer.search(&q, 100, &tables);
                    self.fuzzy_state.as_mut().unwrap().items = items;
                }
            }

            // Printable char: append to query and re-search
            (KeyCode::Char(c), m) if !m.contains(KeyModifiers::CONTROL) => {
                if let Some(fs) = &mut self.fuzzy_state {
                    fs.push_char(c);
                    let q = fs.query.clone();
                    let tables = self.current_sql_tables();
                    let items = self.fuzzy_completer.search(&q, 100, &tables);
                    self.fuzzy_state.as_mut().unwrap().items = items;
                }
            }

            // Any other key: close and re-dispatch
            _ => {
                self.fuzzy_state = None;
                return Box::pin(self.handle_key(key)).await;
            }
        }
        false
    }

    // ── Slash commands ────────────────────────────────────────────────────────

    async fn handle_slash_command(&mut self, cmd: &str) {
        // Dispatch /benchmark separately since it needs async query execution
        if cmd.starts_with("/benchmark") {
            self.history.add(cmd.to_string());
            let (n_runs, query_text) = parse_benchmark_args(cmd);
            self.do_benchmark(n_runs, query_text).await;
            return;
        }

        // Dispatch command aliases (e.g. /qh)
        if let Some(expanded) = expand_command_alias(cmd) {
            // Execute the expanded SQL query as if the user typed it
            if let Some(queries) = crate::query::try_split_queries(&expanded) {
                self.history.add(cmd.to_string());
                self.execute_queries(expanded, queries).await;
            } else {
                self.output.push_line(format!("Error: could not parse expanded query for: {}", cmd));
            }
            return;
        }

        match cmd {
            "/view" => self.open_viewer(),
            "/refresh" | "/refresh_cache" => self.do_refresh(),
            "/help" => self.show_help(),
            _ => {
                match handle_meta_command(&mut self.context, cmd) {
                    Ok(true) => {}
                    Ok(false) => self
                        .output
                        .push_line(format!("Unknown command: {}", cmd)),
                    Err(e) => self.output.push_line(format!("Error: {}", e)),
                }
                self.history.add(cmd.to_string());
            }
        }
    }

    // ── Export ───────────────────────────────────────────────────────────────


    // ── Benchmark ────────────────────────────────────────────────────────────

    /// Run `query_text` N+1 times (1 warmup), collect per-run elapsed times,
    /// and display min/avg/p90/max statistics in the output pane.
    async fn do_benchmark(&mut self, n_runs: usize, query_text: String) {
        if query_text.trim().is_empty() {
            self.output.push_line(
                "Usage: /benchmark [N] <query>  (default N=3, first run is warmup)",
            );
            return;
        }

        let total = n_runs + 1; // 1 warmup + N timed
        self.output.push_line(format!(
            "Benchmarking ({} warmup + {} timed run{}):",
            1,
            n_runs,
            if n_runs == 1 { "" } else { "s" }
        ));
        self.push_sql_echo(query_text.trim());

        let mut times_ms: Vec<f64> = Vec::with_capacity(n_runs);

        for i in 0..total {
            let queries = match crate::query::try_split_queries(query_text.trim()) {
                Some(q) => q,
                None => {
                    self.output.push_line("Error: could not parse query");
                    return;
                }
            };

            // Silence all output during benchmark runs
            let (tx, mut rx) = mpsc::unbounded_channel::<TuiMsg>();
            let mut ctx = self.context.clone();
            // Disable result cache for accurate timing
            if !ctx.args.extra.iter().any(|e| e.starts_with("enable_result_cache=")) {
                ctx.args.extra.push("enable_result_cache=false".to_string());
                ctx.update_url();
            }
            ctx.tui_output_tx = Some(tx);

            let start = Instant::now();
            let handle = tokio::spawn(async move {
                for q in queries {
                    if crate::query::query(&mut ctx, q).await.is_err() {
                        return false;
                    }
                }
                true
            });

            // Drain messages (discard) while the query runs
            loop {
                tokio::select! {
                    msg = rx.recv() => {
                        if msg.is_none() { break; }
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        // Continue draining
                    }
                }
                // Stop when the handle has completed
                if handle.is_finished() {
                    // Drain remaining
                    while rx.try_recv().is_ok() {}
                    break;
                }
            }

            let ok = handle.await.unwrap_or(false);
            let elapsed = start.elapsed().as_secs_f64() * 1000.0;

            if !ok {
                self.output.push_line("Error: query failed during benchmark");
                return;
            }

            if i == 0 {
                self.output.push_line(format!("  warmup: {:.1}ms", elapsed));
            } else {
                self.output.push_line(format!("  run {}: {:.1}ms", i, elapsed));
                times_ms.push(elapsed);
            }
        }

        if times_ms.is_empty() {
            return;
        }

        // Compute statistics
        let min = times_ms.iter().cloned().fold(f64::INFINITY, f64::min);
        let max = times_ms.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let avg = times_ms.iter().sum::<f64>() / times_ms.len() as f64;

        let mut sorted = times_ms.clone();
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let p90_idx = (sorted.len() as f64 * 0.9).ceil() as usize;
        let p90 = sorted[p90_idx.saturating_sub(1).min(sorted.len() - 1)];

        self.output.push_line(format!(
            "Results: min={:.1}ms  avg={:.1}ms  p90={:.1}ms  max={:.1}ms",
            min, avg, p90, max
        ));
    }

    /// Called from handle_key: validates that data is available, then queues
    /// the viewer to open at the top of the next event-loop iteration.
    fn open_viewer(&mut self) {
        if self.context.last_result.is_none() {
            self.set_flash("No results to display — run a query first");
            return;
        }
        if !self.context.args.format.starts_with("client:") {
            self.set_flash("Viewer requires client format — run: set format = client:auto;");
            return;
        }
        self.pending_viewer = true;
    }

    /// Actually launches csvlens. Called at the top of event_loop, outside any
    /// event-handler, so crossterm's global InternalEventReader is fully idle.
    fn run_viewer(&mut self, terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>) {
        // Mirror exactly what run() does on exit: tear down raw mode + alt screen
        // through the ratatui backend's BufWriter so everything is flushed in order.
        let _ = disable_raw_mode();
        let _ = execute!(terminal.backend_mut(), PopKeyboardEnhancementFlags);
        let _ = execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture);
        let _ = std::io::Write::flush(terminal.backend_mut());

        let result = open_csvlens_viewer(&self.context);

        // Mirror exactly what run() does on entry: restore raw mode + alt screen.
        let _ = execute!(terminal.backend_mut(), EnterAlternateScreen, EnableMouseCapture);
        let _ = execute!(
            terminal.backend_mut(),
            PushKeyboardEnhancementFlags(KeyboardEnhancementFlags::DISAMBIGUATE_ESCAPE_CODES)
        );
        let _ = enable_raw_mode();
        let _ = std::io::Write::flush(terminal.backend_mut());
        let _ = terminal.clear();

        if let Err(e) = result {
            self.set_flash(format!("Viewer error: {}", e));
        }
    }

    /// Show a temporary error message in the status bar for ~2 seconds.
    fn set_flash(&mut self, msg: impl Into<String>) {
        self.flash_message = Some((msg.into(), Instant::now()));
    }

    fn do_refresh(&mut self) {
        if self.context.args.no_completion {
            self.output
                .push_line("Auto-completion is disabled. Enable with: set completion = on;");
            return;
        }
        let cache = self.schema_cache.clone();
        let mut ctx_clone = self.context.clone();
        self.output.push_line("Refreshing schema cache...");
        tokio::spawn(async move {
            if let Err(e) = cache.refresh(&mut ctx_clone).await {
                eprintln!("Failed to refresh schema cache: {}", e);
            }
        });
    }

    fn show_help(&mut self) {
        self.output.push_ansi_text(
            "Keyboard shortcuts:\n\
             Ctrl+V          - Open last result in csvlens viewer\n\
             Ctrl+R          - Reverse history search\n\
             Ctrl+Space      - Fuzzy schema search\n\
             Tab             - Open / navigate completion popup\n\
             Shift+Tab       - Navigate completion popup backwards\n\
             Alt+F           - Format SQL\n\
             Ctrl+D          - Exit\n\
             Ctrl+C          - Cancel current input (or running query)\n\
             Page Up/Down    - Scroll output\n\
             \n\
             Special commands:\n\
             \\refresh                    - Manually refresh schema cache\n\
             \\benchmark [N] <query>      - Run query N+1 times (1 warmup), show timing stats\n\
             \\help                       - Show this help message\n\
             \n\
             SQL-style commands:\n\
             set format = <value>;       - Change output format\n\
             unset format;               - Reset format to default\n\
             set completion = on/off;    - Enable/disable auto-completion\n\
             Ctrl+Up/Down    - Cycle history\n\
             Page Up/Down    - Scroll output pane\n\
             Shift+Enter     - Insert newline without submitting",
        );
    }

    // ── Query execution ──────────────────────────────────────────────────────

    /// Echo SQL text to the output pane with the same syntax highlighting as the input pane.
    /// The first line gets the `❯ ` prefix (green+bold); continuation lines get `  `.
    fn push_sql_echo(&mut self, sql: &str) {
        let all_spans = self.highlighter.highlight_to_spans(sql);
        let mut byte_offset = 0usize;
        let mut first_line = true;
        for line_text in sql.lines() {
            let line_start = byte_offset;
            let line_end = byte_offset + line_text.len();
            // Collect spans for this line, clipped and re-offset to line-local coords
            let line_spans: Vec<(std::ops::Range<usize>, ratatui::style::Style)> = all_spans
                .iter()
                .filter(|(r, _)| r.start < line_end && r.end > line_start)
                .map(|(r, style)| {
                    let s = r.start.saturating_sub(line_start);
                    let e = (r.end - line_start).min(line_text.len());
                    (s..e, *style)
                })
                .collect();
            let prefix = if first_line { "❯ " } else { "  " };
            self.output.push_prompt_highlighted(prefix, line_text, &line_spans);
            first_line = false;
            byte_offset += line_text.len() + 1; // +1 for '\n'
        }
    }

    async fn execute_queries(&mut self, original_text: String, queries: Vec<String>) {
        // Echo query to output pane with syntax highlighting
        self.push_sql_echo(original_text.trim());

        // Show custom settings (from /set commands) between query echo and result
        let display_extras: Vec<String> = self
            .context
            .args
            .extra
            .iter()
            .filter(|e| {
                // Skip params that are always present as part of the URL construction
                !e.starts_with("database=")
                    && !e.starts_with("format=")
                    && !e.starts_with("query_label=")
                    && !e.starts_with("advanced_mode=")
                    && !e.starts_with("output_format=")
            })
            .map(|e| url_decode_setting(e))
            .collect();
        if !display_extras.is_empty() {
            self.output.push_stat(&format!("  Settings: {}", display_extras.join(", ")));
        }

        let (tx, rx) = mpsc::unbounded_channel::<TuiMsg>();
        let cancel_token = CancellationToken::new();

        self.query_rx = Some(rx);
        self.cancel_token = Some(cancel_token.clone());
        self.is_running = true;
        self.running_hint.clear();
        self.progress_rows = 0;
        self.query_start = Some(Instant::now());
        self.pending_schema_refresh = false;

        // Build a context clone with the TUI output channel attached
        let mut ctx = self.context.clone();
        ctx.tui_output_tx = Some(tx);
        ctx.query_cancel = Some(cancel_token);

        // Sync completer enabled state
        self.completer.set_enabled(!self.context.args.no_completion);

        tokio::spawn(async move {
            for q in queries {
                if query(&mut ctx, q).await.is_err() {
                    // Error message already sent through the channel by query()
                    return;
                }
            }
            // `ctx` is dropped here → `tui_output_tx` is dropped → channel disconnected
        });
    }

    // ── Textarea helpers ─────────────────────────────────────────────────────

    /// Format the current textarea content as SQL (Alt+F).
    fn format_sql(&mut self) {
        let sql = self.textarea.lines().join("\n");
        if sql.trim().is_empty() {
            return;
        }
        let options = sqlformat::FormatOptions {
            indent: sqlformat::Indent::Spaces(2),
            uppercase: Some(true),
            ..sqlformat::FormatOptions::default()
        };
        let formatted = sqlformat::format(&sql, &sqlformat::QueryParams::None, &options);
        if formatted != sql {
            self.set_textarea_content(&formatted);
        }
    }

    fn reset_textarea(&mut self) {
        self.textarea = Self::make_textarea();
    }

    fn set_textarea_content(&mut self, content: &str) {
        let lines: Vec<String> = content.lines().map(|l| l.to_string()).collect();
        let mut ta = TextArea::new(if lines.is_empty() {
            vec![String::new()]
        } else {
            lines
        });
        self.textarea = ta;
        // Move cursor to end of content
        self.textarea.move_cursor(CursorMove::Bottom);
        self.textarea.move_cursor(CursorMove::End);
    }

    // ── Rendering ────────────────────────────────────────────────────────────

    fn render(&mut self, f: &mut ratatui::Frame) {
        let area = f.area();

        // Only highlight the current line when there are multiple lines — on a
        // single-line textarea the highlight adds noise with no benefit.
        let cursor_line_bg = if self.textarea.lines().len() > 1 {
            ratatui::style::Style::default().bg(ratatui::style::Color::Indexed(234))
        } else {
            ratatui::style::Style::default()
        };
        self.textarea.set_cursor_line_style(cursor_line_bg);

        let input_height = if self.is_running {
            // Running pane: spinner row + hint row + top/bottom borders = 4
            4u16
        } else {
            // Input pane: textarea lines + 2 border rows, at least 3
            let ta_lines = self.textarea.lines().len() as u16;
            (ta_lines + 2).clamp(3, area.height / 3)
        };

        let layout = compute_layout(area, input_height);

        // Clamp scroll so it stays in bounds
        self.output.clamp_scroll(layout.output.height);

        // Output pane
        self.output.render(layout.output, f.buffer_mut());

        if self.is_running {
            self.render_running_pane(f, layout.input);
        } else {
            // Input area: outer block provides top + bottom separator lines
            let outer_block = Block::default()
                .borders(Borders::TOP | Borders::BOTTOM)
                .border_style(Style::default().fg(Color::DarkGray));
            let inner = outer_block.inner(layout.input);
            f.render_widget(outer_block, layout.input);

            // Split inner area: prompt column + textarea
            let chunks = Layout::horizontal([
                Constraint::Length(2), // "❯ " prompt
                Constraint::Min(0),
            ])
            .split(inner);

            let prompt = Paragraph::new("❯").style(Style::default().fg(Color::Green));
            f.render_widget(prompt, chunks[0]);
            f.render_widget(&self.textarea, chunks[1]);

            // Apply per-token syntax highlighting to the rendered textarea buffer
            let textarea_area = chunks[1];
            self.apply_textarea_highlights(f.buffer_mut(), textarea_area);

            // Render completion popup if open (not during history search)
            if self.history_search.is_none() {
                if let Some(cs) = &self.completion_state {
                    if !cs.is_empty() {
                        let popup_rect = completion_popup::popup_area(
                            cs,
                            layout.input,
                            chunks[1].x,
                            area,
                        );
                        completion_popup::render(cs, popup_rect, f);
                    }
                }
            }
        }

        // History search popup (above input area, like fuzzy popup)
        if self.history_search.is_some() {
            let popup_h = (history_search::MAX_VISIBLE as u16 + 4).min(layout.input.y);
            if popup_h > 2 {
                let popup_rect = Rect::new(
                    0,
                    layout.input.y.saturating_sub(popup_h),
                    area.width,
                    popup_h,
                );
                // Borrow separately to avoid simultaneous &self + &mut self
                let hs = self.history_search.as_ref().unwrap();
                let entries: Vec<String> = self.history.entries().to_vec();
                Self::render_history_search_popup(f, popup_rect, hs, &entries, &self.highlighter);
            }
        }

        // Signature hint popup (above input, near where the function name starts)
        if let Some((func_name, sigs)) = &self.signature_hint {
            if self.history_search.is_none() && self.fuzzy_state.is_none() {
                let func_name = func_name.clone();
                let sigs = sigs.clone();
                Self::render_signature_hint(f, layout.input, area, &func_name, &sigs);
            }
        }

        // Fuzzy search overlay (rendered on top of everything)
        if let Some(fs) = &self.fuzzy_state {
            let fuzzy_rect = fuzzy_popup::popup_area(layout.input, area);
            if fuzzy_rect.height > 2 {
                fuzzy_popup::render(fs, fuzzy_rect, f);
            }
        }

        // Ctrl+H help overlay (topmost — rendered above all other content)
        if self.help_visible {
            self.render_help_popup(f, area);
        }

        // Status bar
        self.render_status_bar(f, layout.status);
    }

    /// Apply syntax highlighting to the textarea by post-processing the ratatui buffer.
    ///
    /// After `tui-textarea` renders its content (cursor, selection, etc.) we walk
    /// through the buffer cells corresponding to the textarea's screen area and
    /// patch the foreground colour of "plain" text cells.  We skip the cursor
    /// character so the block-cursor stays visible.
    fn apply_textarea_highlights(
        &self,
        buf: &mut ratatui::buffer::Buffer,
        area: ratatui::layout::Rect,
    ) {
        let lines = self.textarea.lines();
        let full_text = lines.join("\n");
        let spans = self.highlighter.highlight_to_spans(&full_text);
        if spans.is_empty() {
            return;
        }

        let (cursor_row, cursor_col) = self.textarea.cursor();

        let mut byte_offset = 0usize;
        for (line_idx, line) in lines.iter().enumerate() {
            let screen_y = area.y + line_idx as u16;
            if screen_y >= area.y + area.height {
                break;
            }

            let mut char_col = 0usize;
            for (byte_in_line, _ch) in line.char_indices() {
                let byte_pos = byte_offset + byte_in_line;
                let screen_x = area.x + char_col as u16;
                if screen_x >= area.x + area.width {
                    break;
                }

                // Skip the cursor character — it has special styling (REVERSED)
                // that we want to preserve exactly as tui-textarea rendered it.
                if line_idx == cursor_row && char_col == cursor_col {
                    char_col += 1;
                    continue;
                }

                // Find the highest-priority span covering this byte position.
                if let Some((_, style)) =
                    spans.iter().find(|(r, _)| r.start <= byte_pos && byte_pos < r.end)
                {
                    let pos = ratatui::layout::Position::new(screen_x, screen_y);
                    if let Some(cell) = buf.cell_mut(pos) {
                        // Patch only the foreground colour; preserve bg, modifiers etc.
                        let current = cell.style();
                        cell.set_style(current.patch(*style));
                    }
                }
                char_col += 1;
            }

            byte_offset += line.len() + 1; // +1 for the '\n' joining lines
        }
    }

    fn render_running_pane(&self, f: &mut ratatui::Frame, area: Rect) {
        let outer_block = Block::default()
            .borders(Borders::TOP | Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray));
        let inner = outer_block.inner(area);
        f.render_widget(outer_block, area);

        let spinner = SPINNER_FRAMES[(self.spinner_tick / 2) as usize % SPINNER_FRAMES.len()];
        let elapsed = if let Some(start) = self.query_start {
            if self.progress_rows > 0 {
                format!(
                    "{} {:.1}s  {:>} rows received",
                    spinner,
                    start.elapsed().as_secs_f64(),
                    format_with_commas(self.progress_rows),
                )
            } else {
                format!("{} {:.1}s", spinner, start.elapsed().as_secs_f64())
            }
        } else {
            spinner.to_string()
        };

        let mut lines = vec![
            Line::from(Span::styled(elapsed, Style::default().fg(Color::Yellow))),
        ];
        if !self.running_hint.is_empty() {
            lines.push(Line::from(Span::styled(
                self.running_hint.clone(),
                Style::default().fg(Color::DarkGray),
            )));
        }

        f.render_widget(Paragraph::new(lines), inner);
    }

    fn render_history_search_popup(
        f: &mut ratatui::Frame,
        area: Rect,
        hs: &HistorySearch,
        entries: &[String],
        highlighter: &SqlHighlighter,
    ) {
        use ratatui::{
            style::Modifier,
            widgets::{Clear, List, ListItem},
        };

        f.render_widget(Clear, area);

        let block = Block::default()
            .title(" History Search  (Enter accept · ↑/↓ navigate · Ctrl+R older · Esc close) ")
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Cyan));

        let inner = block.inner(area);
        f.render_widget(block, area);

        // Bottom-up layout: match list above, search line at bottom
        let chunks = Layout::vertical([
            Constraint::Min(0),    // match list (fills remaining space)
            Constraint::Length(1), // search input line
        ])
        .split(inner);

        // ── Search line (bottom) ────────────────────────────────────────────
        let search_line = Line::from(vec![
            Span::styled("/ ", Style::default().fg(Color::Cyan).add_modifier(Modifier::BOLD)),
            Span::styled(hs.query().to_string(), Style::default().fg(Color::White)),
            Span::styled("█", Style::default().fg(Color::Cyan)),
        ]);
        f.render_widget(Paragraph::new(search_line), chunks[1]);

        // ── Match list (above, rendered bottom-to-top) ─────────────────────
        let list_h = chunks[0].height as usize;
        let list_w = chunks[0].width as usize;
        let all = hs.all_matches();
        let offset = hs.scroll_offset();

        // Collect the visible slice then reverse it so most-recent is at bottom
        let visible: Vec<(usize, usize)> = all
            .iter()
            .enumerate()
            .skip(offset)
            .take(list_h)
            .map(|(idx, &entry_idx)| (idx, entry_idx))
            .collect::<Vec<_>>()
            .into_iter()
            .rev()
            .collect();

        // Pad with blank rows at the top so results are bottom-aligned
        let pad = list_h.saturating_sub(visible.len());
        let mut items: Vec<ListItem> = (0..pad)
            .map(|_| ListItem::new(Line::from("")))
            .collect();

        for (idx, entry_idx) in &visible {
            let is_sel = *idx == hs.selected();
            let entry = &entries[*entry_idx];
            let display = history_search::format_entry_oneline(entry, list_w.saturating_sub(1));

            let line = if is_sel {
                // Selected row: solid cyan background, no syntax colours
                let sel_style = Style::default()
                    .fg(Color::Black)
                    .bg(Color::Cyan)
                    .add_modifier(Modifier::BOLD);
                Line::from(Span::styled(display, sel_style))
            } else {
                // Normal row: apply SQL syntax highlighting to the display text
                let spans_meta = highlighter.highlight_to_spans(&display);
                if spans_meta.is_empty() {
                    Line::from(Span::styled(display, Style::default().fg(Color::White)))
                } else {
                    let mut parts: Vec<Span> = Vec::new();
                    let mut last = 0usize;
                    for (range, style) in &spans_meta {
                        let s = range.start.min(display.len());
                        let e = range.end.min(display.len());
                        if s > last {
                            parts.push(Span::styled(
                                display[last..s].to_string(),
                                Style::default().fg(Color::White),
                            ));
                        }
                        if s < e {
                            parts.push(Span::styled(display[s..e].to_string(), *style));
                        }
                        last = e;
                    }
                    if last < display.len() {
                        parts.push(Span::styled(
                            display[last..].to_string(),
                            Style::default().fg(Color::White),
                        ));
                    }
                    Line::from(parts)
                }
            };
            items.push(ListItem::new(line));
        }

        f.render_widget(List::new(items), chunks[0]);
    }

    fn render_signature_hint(
        f: &mut ratatui::Frame,
        input_area: Rect,
        total: Rect,
        func_name: &str,
        sigs: &[String],
    ) {
        use ratatui::{
            style::Modifier,
            widgets::{Clear, List, ListItem},
        };

        // Build display lines
        let lines: Vec<String> = if sigs.is_empty() {
            vec![format!("{}(...)", func_name)]
        } else {
            sigs.to_vec()
        };

        let n = lines.len() as u16;
        let popup_h = n + 2; // content rows + 2 borders
        let popup_w = {
            let max_w = lines.iter().map(|l| l.len()).max().unwrap_or(10) as u16 + 4;
            max_w.min(total.width.saturating_sub(4))
        };

        // Anchor: just above the input area, right-aligned within the terminal
        let y = input_area.y.saturating_sub(popup_h);
        let x = total.width.saturating_sub(popup_w + 1);
        let rect = Rect::new(x, y, popup_w, popup_h);

        f.render_widget(Clear, rect);

        let title = Span::styled(
            format!(" {} ", func_name),
            Style::default()
                .fg(Color::LightCyan)
                .add_modifier(Modifier::BOLD),
        );
        let block = Block::default()
            .title(title)
            .borders(Borders::ALL)
            .border_style(Style::default().fg(Color::Indexed(67)));

        let inner = block.inner(rect);
        f.render_widget(block, rect);

        let items: Vec<ListItem> = lines
            .iter()
            .map(|sig| {
                // Colour: function name in cyan, parens+types in default
                let paren = sig.find('(').unwrap_or(sig.len());
                let (fname, rest) = sig.split_at(paren);
                ListItem::new(Line::from(vec![
                    Span::styled(fname.to_string(), Style::default().fg(Color::LightCyan)),
                    Span::styled(rest.to_string(), Style::default().fg(Color::White)),
                ]))
            })
            .collect();

        f.render_widget(List::new(items), inner);
    }

    fn render_help_popup(&self, f: &mut ratatui::Frame, area: Rect) {
        use ratatui::{
            style::Modifier,
            text::Span,
            widgets::{BorderType, Clear, Paragraph, Wrap},
        };

        // Build styled help lines
        let section_style = Style::default()
            .fg(Color::LightBlue)
            .add_modifier(Modifier::BOLD);
        let sep_style = Style::default().fg(Color::Indexed(240));
        let key_style = Style::default().fg(Color::LightYellow);
        let desc_style = Style::default().fg(Color::White);
        let cmd_style = Style::default().fg(Color::LightCyan);

        let keybinds: &[(&str, &str)] = &[
            ("Ctrl+D",        "Exit"),
            ("Ctrl+C",        "Cancel input / cancel running query"),
            ("Ctrl+V",        "Open last result in csvlens viewer"),
            ("Ctrl+R",        "Reverse history search"),
            ("Ctrl+Space",    "Fuzzy schema search"),
            ("Tab",           "Open / navigate completion popup"),
            ("Shift+Tab",     "Navigate completion popup backwards"),
            ("Alt+F",         "Format SQL (uppercase keywords, 2-space indent)"),
            ("Page Up/Down",  "Scroll output pane"),
            ("Ctrl+Up/Down",  "Cycle through history"),
            ("Escape",        "Close any open popup"),
        ];
        let commands: &[(&str, &str)] = &[
            ("/help",              "Show this help"),
            ("/set k=v",          "Set a query parameter"),
            ("/unset k",          "Remove a query parameter"),
            ("/benchmark [N]",    "Run query N times (default 3) and report timings"),
            ("/qh [limit] [min]", "Query history (default: last 10 in 60 min)"),
        ];

        // Determine column widths
        let key_col = keybinds.iter().chain(commands.iter()).map(|(k, _)| k.len()).max().unwrap_or(14) + 2;

        let mut lines: Vec<Line> = Vec::new();

        // Section: keyboard shortcuts
        lines.push(Line::from(Span::styled("  Keyboard shortcuts", section_style)));
        lines.push(Line::from(Span::styled(
            format!("  {}", "─".repeat(key_col + 30)),
            sep_style,
        )));
        for (key, desc) in keybinds {
            let padding = key_col.saturating_sub(key.len());
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(key.to_string(), key_style),
                Span::raw(" ".repeat(padding)),
                Span::styled(desc.to_string(), desc_style),
            ]));
        }

        lines.push(Line::from(""));

        // Section: special commands
        lines.push(Line::from(Span::styled("  Special commands", section_style)));
        lines.push(Line::from(Span::styled(
            format!("  {}", "─".repeat(key_col + 30)),
            sep_style,
        )));
        for (cmd, desc) in commands {
            let padding = key_col.saturating_sub(cmd.len());
            lines.push(Line::from(vec![
                Span::raw("  "),
                Span::styled(cmd.to_string(), cmd_style),
                Span::raw(" ".repeat(padding)),
                Span::styled(desc.to_string(), desc_style),
            ]));
        }

        lines.push(Line::from(""));
        lines.push(Line::from(vec![
            Span::raw("  "),
            Span::styled("Press Esc or q to close", sep_style),
        ]));

        // Sizing: fixed inner content width + borders
        let content_w = (key_col + 2 + 46) as u16 + 4; // key col + desc + padding + borders
        let content_h = lines.len() as u16 + 2;

        let popup_w = content_w.min(area.width.saturating_sub(6));
        let popup_h = content_h.min(area.height.saturating_sub(4));
        let x = area.x + area.width.saturating_sub(popup_w) / 2;
        let y = area.y + area.height.saturating_sub(popup_h) / 2;
        let popup_rect = Rect::new(x, y, popup_w, popup_h);

        // Shadow: render a dark rect shifted 1 cell right + 1 cell down
        if popup_rect.right() + 1 < area.right() && popup_rect.bottom() + 1 < area.bottom() {
            let shadow_rect = Rect::new(
                popup_rect.x + 1,
                popup_rect.y + 1,
                popup_rect.width,
                popup_rect.height,
            );
            f.render_widget(
                Paragraph::new("").style(Style::default().bg(Color::Indexed(232))),
                shadow_rect,
            );
        }

        // Main popup background + border
        f.render_widget(Clear, popup_rect);
        let block = Block::default()
            .title(Span::styled(
                " ✦ Help  ·  Esc / q to close ",
                Style::default().fg(Color::LightBlue).add_modifier(Modifier::BOLD),
            ))
            .borders(Borders::ALL)
            .border_type(BorderType::Double)
            .border_style(Style::default().fg(Color::Indexed(67))) // steel blue
            .style(Style::default().bg(Color::Indexed(234)));       // dark background

        let inner = block.inner(popup_rect);
        f.render_widget(block, popup_rect);
        f.render_widget(
            Paragraph::new(lines)
                .wrap(Wrap { trim: false })
                .style(Style::default().bg(Color::Indexed(234))),
            inner,
        );
    }

    fn render_status_bar(&mut self, f: &mut ratatui::Frame, area: Rect) {
        let host = &self.context.args.host;
        let db = &self.context.args.database;

        let conn_info = format!(" {} | {}", host, db);

        // Expire flash messages older than 2 seconds.
        if let Some((_, t)) = &self.flash_message {
            if t.elapsed() >= Duration::from_secs(2) {
                self.flash_message = None;
            }
        }

        let right = if self.is_running {
            " Ctrl+C cancel ".to_string()
        } else if self.history_search.is_some() {
            " Enter accept  Ctrl+R older  Esc cancel ".to_string()
        } else if self.fuzzy_state.is_some() {
            " Enter accept  ↑/↓ navigate  Esc close ".to_string()
        } else if self.completion_state.is_some() {
            " Enter accept  Tab/↑/↓ navigate  Esc close ".to_string()
        } else {
            " Ctrl+D exit  Ctrl+V viewer  Ctrl+Space fuzzy  Tab complete  Alt+F format  Ctrl+H help ".to_string()
        };

        let total = area.width as usize;

        let status = if let Some((msg, _)) = &self.flash_message {
            // Flash: error on the left in red, hint line on the right in normal style.
            let flash_left = format!(" {} ", msg);
            let pad = total.saturating_sub(flash_left.len() + right.len());
            let gap = " ".repeat(pad);
            let spans: Vec<Span> = vec![
                Span::styled(flash_left + &gap, Style::default().bg(Color::Red).fg(Color::White)),
                Span::styled(right, Style::default().bg(Color::DarkGray).fg(Color::White)),
            ];
            Paragraph::new(Line::from(spans))
        } else {
            let pad = total.saturating_sub(conn_info.len() + right.len());
            let text = format!("{}{}{}", conn_info, " ".repeat(pad), right);
            Paragraph::new(text).style(Style::default().bg(Color::DarkGray).fg(Color::White))
        };
        f.render_widget(status, area);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_benchmark_args_default() {
        let (n, q) = parse_benchmark_args("/benchmark SELECT 1");
        assert_eq!(n, 3);
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn test_parse_benchmark_args_explicit_n() {
        let (n, q) = parse_benchmark_args("/benchmark 5 SELECT 1");
        assert_eq!(n, 5);
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn test_parse_benchmark_args_no_query() {
        let (n, q) = parse_benchmark_args("/benchmark");
        assert_eq!(n, 3);
        assert_eq!(q, "");
    }

    #[test]
    fn test_parse_benchmark_args_min_n() {
        // 0 should be clamped to 1
        let (n, _) = parse_benchmark_args("/benchmark 0 SELECT 1");
        assert_eq!(n, 1);
    }

    #[test]
    fn test_format_with_commas() {
        assert_eq!(format_with_commas(0), "0");
        assert_eq!(format_with_commas(999), "999");
        assert_eq!(format_with_commas(1000), "1,000");
        assert_eq!(format_with_commas(1234567), "1,234,567");
    }
}
