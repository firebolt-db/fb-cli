pub mod completion_popup;
pub mod fuzzy_popup;
pub mod history;
pub mod history_search;
pub mod layout;
pub mod output_pane;

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

/// Parse `\benchmark [N] <query>` arguments.
/// Returns `(n_runs, query_text)` where `n_runs` defaults to 3.
fn parse_benchmark_args(cmd: &str) -> (usize, String) {
    let rest = cmd
        .strip_prefix("\\benchmark")
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

    /// Active tab-completion popup session; `None` when the popup is closed.
    completion_state: Option<CompletionState>,

    /// Active Ctrl+Space fuzzy search session; `None` when closed.
    fuzzy_state: Option<FuzzyState>,

    /// Active Ctrl+R reverse-search session; `None` when not searching.
    history_search: Option<HistorySearch>,

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
            completion_state: None,
            fuzzy_state: None,
            history_search: None,
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
                    break;
                }
            }
        }
    }

    // ── Key handling ─────────────────────────────────────────────────────────

    /// Returns `true` when the app should exit.
    async fn handle_key(&mut self, key: crossterm::event::KeyEvent) -> bool {
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
                self.history_search = Some(HistorySearch::new(saved));
            }

            // ── Cancel / clear ────────────────────────────────────────────
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.completion_state = None;
                self.reset_textarea();
                self.output.push_line("^C");
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

                // Backslash meta-commands
                if trimmed.starts_with('\\') {
                    self.handle_backslash_command(&trimmed).await;
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

        match (key.code, key.modifiers) {
            // Ctrl+R: cycle to next older match
            (KeyCode::Char('r'), m) if m.contains(KeyModifiers::CONTROL) => {
                if let Some(hs) = &mut self.history_search {
                    hs.search_older(&entries);
                }
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

            // Enter: accept the current match
            (KeyCode::Enter, _) => {
                let matched = self
                    .history_search
                    .as_ref()
                    .and_then(|hs| hs.matched(&entries))
                    .map(|s| s.to_string());
                self.history_search = None;
                let content = matched.unwrap_or_default();
                self.set_textarea_content(&content);
            }

            // Backspace: remove one character from the query
            (KeyCode::Backspace, _) => {
                if let Some(hs) = &mut self.history_search {
                    hs.pop_char(&entries);
                }
            }

            // Printable character: append to search query
            (KeyCode::Char(c), m) if !m.contains(KeyModifiers::CONTROL) => {
                if let Some(hs) = &mut self.history_search {
                    hs.push_char(c, &entries);
                }
            }

            // Any other key: accept match, exit search, re-process key normally
            _ => {
                let matched = self
                    .history_search
                    .as_ref()
                    .and_then(|hs| hs.matched(&entries))
                    .map(|s| s.to_string());
                self.history_search = None;
                if let Some(content) = matched {
                    self.set_textarea_content(&content);
                }
                // Re-dispatch to normal handler
                return Box::pin(self.handle_key(key)).await;
            }
        }

        false
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

        let (word_start_byte_in_line, items) =
            self.completer.complete_at(line_to_cursor, cursor_col);

        if items.is_empty() {
            return;
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

    // ── Backslash commands ───────────────────────────────────────────────────

    async fn handle_backslash_command(&mut self, cmd: &str) {
        // Dispatch \export: write last result to a file
        if cmd.starts_with("\\export") {
            self.history.add(cmd.to_string());
            self.do_export(cmd);
            return;
        }

        // Dispatch \benchmark separately since it needs async query execution
        if cmd.starts_with("\\benchmark") {
            self.history.add(cmd.to_string());
            let (n_runs, query_text) = parse_benchmark_args(cmd);
            self.do_benchmark(n_runs, query_text).await;
            return;
        }

        match cmd {
            "\\view" => self.open_viewer(),
            "\\refresh" | "\\refresh_cache" => self.do_refresh(),
            "\\help" => self.show_help(),
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

    /// Handle `\export <file> [format]` — write the last query result to a file.
    /// Supported formats: csv (default), tsv, json, jsonlines.
    fn do_export(&mut self, cmd: &str) {
        // Parse: \export <path> [format]
        let rest = cmd.strip_prefix("\\export").unwrap_or("").trim();
        if rest.is_empty() {
            self.output.push_line("Usage: \\export <file> [csv|tsv|json|jsonlines]");
            return;
        }

        let mut parts = rest.splitn(2, char::is_whitespace);
        let path_str = parts.next().unwrap_or("").trim().to_string();
        let format = parts
            .next()
            .map(|s| s.trim().to_lowercase())
            .unwrap_or_else(|| "csv".to_string());

        let result = match &self.context.last_result {
            Some(r) => r.clone(),
            None => {
                self.set_flash("No results to export — run a query first");
                return;
            }
        };

        use crate::table_renderer::write_result_as_csv;
        use std::fs::File;
        use std::io::{BufWriter, Write};

        let file = match File::create(&path_str) {
            Ok(f) => f,
            Err(e) => {
                self.output
                    .push_line(format!("Error: could not create file '{}': {}", path_str, e));
                return;
            }
        };
        let mut writer = BufWriter::new(file);

        // Helper: format a JSON value as a plain string for TSV output
        fn val_to_str(v: &serde_json::Value) -> String {
            match v {
                serde_json::Value::Null => String::new(),
                serde_json::Value::String(s) => s.clone(),
                other => other.to_string(),
            }
        }

        let write_result = match format.as_str() {
            "tsv" => {
                // Tab-separated with header
                let header: Vec<&str> = result.columns.iter().map(|c| c.name.as_str()).collect();
                let _ = writeln!(writer, "{}", header.join("\t"));
                for row in &result.rows {
                    let values: Vec<String> = row.iter().map(val_to_str).collect();
                    let _ = writeln!(writer, "{}", values.join("\t"));
                }
                writer.flush()
            }
            "json" => {
                // JSON array of objects
                let rows_json: Vec<serde_json::Value> = result
                    .rows
                    .iter()
                    .map(|row| {
                        let obj: serde_json::Map<String, serde_json::Value> = result
                            .columns
                            .iter()
                            .zip(row.iter())
                            .map(|(col, val)| (col.name.clone(), val.clone()))
                            .collect();
                        serde_json::Value::Object(obj)
                    })
                    .collect();
                let _ = writeln!(writer, "{}", serde_json::to_string_pretty(&rows_json).unwrap_or_default());
                writer.flush()
            }
            "jsonlines" | "jsonl" | "ndjson" => {
                // One JSON object per line
                for row in &result.rows {
                    let obj: serde_json::Map<String, serde_json::Value> = result
                        .columns
                        .iter()
                        .zip(row.iter())
                        .map(|(col, val)| (col.name.clone(), val.clone()))
                        .collect();
                    let _ = writeln!(writer, "{}", serde_json::to_string(&serde_json::Value::Object(obj)).unwrap_or_default());
                }
                writer.flush()
            }
            _ => {
                // Default: CSV
                write_result_as_csv(&mut writer, &result.columns, &result.rows).map_err(|e| {
                    std::io::Error::new(std::io::ErrorKind::Other, e.to_string())
                })
            }
        };

        match write_result {
            Ok(()) => {
                self.output.push_line(format!(
                    "Exported {} rows to '{}' ({} format)",
                    result.rows.len(),
                    path_str,
                    format
                ));
            }
            Err(e) => {
                self.output
                    .push_line(format!("Error writing to '{}': {}", path_str, e));
            }
        }
    }

    // ── Benchmark ────────────────────────────────────────────────────────────

    /// Run `query_text` N+1 times (1 warmup), collect per-run elapsed times,
    /// and display min/avg/p90/max statistics in the output pane.
    async fn do_benchmark(&mut self, n_runs: usize, query_text: String) {
        if query_text.trim().is_empty() {
            self.output.push_line(
                "Usage: \\benchmark [N] <query>  (default N=3, first run is warmup)",
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
             Ctrl+D          - Exit\n\
             Ctrl+C          - Cancel current input (or running query)\n\
             Page Up/Down    - Scroll output\n\
             \n\
             Special commands:\n\
             \\refresh                    - Manually refresh schema cache\n\
             \\benchmark [N] <query>      - Run query N+1 times (1 warmup), show timing stats\n\
             \\export <file> [fmt]        - Export last result to file (csv/tsv/json/jsonlines)\n\
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

        let (tx, rx) = mpsc::unbounded_channel::<TuiMsg>();
        let cancel_token = CancellationToken::new();

        self.query_rx = Some(rx);
        self.cancel_token = Some(cancel_token.clone());
        self.is_running = true;
        self.running_hint.clear();
        self.progress_rows = 0;
        self.query_start = Some(Instant::now());

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
        } else if let Some(hs) = &self.history_search {
            self.render_history_search_pane(f, layout.input, hs);
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

            // Render completion popup if open
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

        // Fuzzy search overlay (rendered on top of everything)
        if let Some(fs) = &self.fuzzy_state {
            let fuzzy_rect = fuzzy_popup::popup_area(layout.input, area);
            if fuzzy_rect.height > 2 {
                fuzzy_popup::render(fs, fuzzy_rect, f);
            }
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

    fn render_history_search_pane(&self, f: &mut ratatui::Frame, area: Rect, hs: &HistorySearch) {
        let outer_block = Block::default()
            .borders(Borders::TOP | Borders::BOTTOM)
            .border_style(Style::default().fg(Color::Cyan));
        let inner = outer_block.inner(area);
        f.render_widget(outer_block, area);

        let entries = self.history.entries();
        let matched = hs.matched(entries).unwrap_or("");

        // First line: "(reverse-i-search)`query':"
        let query_line = Line::from(vec![
            Span::styled("(reverse-i-search)`", Style::default().fg(Color::DarkGray)),
            Span::styled(hs.query().to_string(), Style::default().fg(Color::Cyan).add_modifier(ratatui::style::Modifier::BOLD)),
            Span::styled("':", Style::default().fg(Color::DarkGray)),
        ]);

        // Second line: the matched entry (truncated to fit)
        let width = inner.width as usize;
        let display: String = matched.chars().take(width).collect();
        let matched_line = Line::from(Span::styled(display, Style::default().fg(Color::Yellow)));

        f.render_widget(Paragraph::new(vec![query_line, matched_line]), inner);
    }

    fn render_status_bar(&mut self, f: &mut ratatui::Frame, area: Rect) {
        let host = &self.context.args.host;
        let db = &self.context.args.database;

        let conn_info = format!(" {} | {} | v{}", host, db, CLI_VERSION);

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
            " Ctrl+D exit  Ctrl+V viewer  Ctrl+Space fuzzy  Tab complete ".to_string()
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
        let (n, q) = parse_benchmark_args("\\benchmark SELECT 1");
        assert_eq!(n, 3);
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn test_parse_benchmark_args_explicit_n() {
        let (n, q) = parse_benchmark_args("\\benchmark 5 SELECT 1");
        assert_eq!(n, 5);
        assert_eq!(q, "SELECT 1");
    }

    #[test]
    fn test_parse_benchmark_args_no_query() {
        let (n, q) = parse_benchmark_args("\\benchmark");
        assert_eq!(n, 3);
        assert_eq!(q, "");
    }

    #[test]
    fn test_parse_benchmark_args_min_n() {
        // 0 should be clamped to 1
        let (n, _) = parse_benchmark_args("\\benchmark 0 SELECT 1");
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
