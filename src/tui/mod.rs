pub mod history;
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

use crate::completion::schema_cache::SchemaCache;
use crate::completion::usage_tracker::UsageTracker;
use crate::completion::SqlCompleter;
use crate::context::Context;
use crate::meta_commands::handle_meta_command;
use crate::query::{query, try_split_queries};
use crate::viewer::open_csvlens_viewer;
use crate::CLI_VERSION;

use history::History;
use layout::compute_layout;
use output_pane::OutputPane;

const SPINNER_FRAMES: &[&str] = &["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"];

pub struct TuiApp {
    context: Context,
    textarea: TextArea<'static>,
    output: OutputPane,
    history: History,
    schema_cache: Arc<SchemaCache>,
    completer: SqlCompleter,

    // Query execution state
    query_rx: Option<mpsc::UnboundedReceiver<TuiMsg>>,
    cancel_token: Option<CancellationToken>,
    is_running: bool,
    query_start: Option<Instant>,
    spinner_tick: u64,
    running_hint: String, // e.g. "Showing first N rows — collecting remainder..."

    // After leaving alt-screen for csvlens we need a full redraw
    needs_clear: bool,
    should_quit: bool,
    pub has_error: bool,
}

impl TuiApp {
    pub fn new(context: Context, schema_cache: Arc<SchemaCache>) -> Self {
        let usage_tracker = context
            .usage_tracker
            .clone()
            .unwrap_or_else(|| Arc::new(UsageTracker::new(10)));
        let completer =
            SqlCompleter::new(schema_cache.clone(), usage_tracker, !context.args.no_completion);

        let history_path = crate::utils::history_path().unwrap_or_default();
        let mut history = History::new(history_path);
        let _ = history.load();

        let textarea = Self::make_textarea();

        Self {
            context,
            textarea,
            output: OutputPane::new(),
            history,
            schema_cache,
            completer,
            query_rx: None,
            cancel_token: None,
            is_running: false,
            query_start: None,
            spinner_tick: 0,
            running_hint: String::new(),
            needs_clear: false,
            should_quit: false,
            has_error: false,
        }
    }

    fn make_textarea() -> TextArea<'static> {
        TextArea::default()
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

        match (key.code, key.modifiers) {
            // ── Exit ──────────────────────────────────────────────────────
            (KeyCode::Char('d'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.should_quit = true;
                return true;
            }

            // ── Cancel / clear ────────────────────────────────────────────
            (KeyCode::Char('c'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.reset_textarea();
                self.output.push_line("^C");
                self.history.reset_navigation();
            }

            // ── Ctrl+V: open csvlens ──────────────────────────────────────
            (KeyCode::Char('v'), m) if m.contains(KeyModifiers::CONTROL) => {
                self.open_viewer();
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

    // ── Backslash commands ───────────────────────────────────────────────────

    async fn handle_backslash_command(&mut self, cmd: &str) {
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

    fn open_viewer(&mut self) {
        if self.context.last_result.is_none() {
            self.output
                .push_line("No query results to display. Run a query first.");
            return;
        }

        // Temporarily leave the TUI to let csvlens take over the terminal.
        let _ = disable_raw_mode();
        let _ = execute!(std::io::stdout(), LeaveAlternateScreen);

        if let Err(e) = open_csvlens_viewer(&self.context) {
            eprintln!("Error: {}", e);
        }

        let _ = enable_raw_mode();
        let _ = execute!(std::io::stdout(), EnterAlternateScreen);
        self.needs_clear = true;
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
            "Special commands:\n\
             \\view           - Open last query result in csvlens viewer\n\
             \\refresh        - Manually refresh schema cache\n\
             \\help           - Show this help message\n\
             \n\
             SQL-style commands:\n\
             set format = <value>;       - Change output format\n\
             unset format;               - Reset format to default\n\
             set completion = on/off;    - Enable/disable auto-completion\n\
             \n\
             Keyboard shortcuts:\n\
             Ctrl+V          - Open csvlens viewer for last result\n\
             Ctrl+D          - Exit\n\
             Ctrl+C          - Cancel current input (or running query)\n\
             Ctrl+Up/Down    - Cycle history\n\
             Page Up/Down    - Scroll output pane\n\
             Enter           - Submit query (when complete) or insert newline",
        );
    }

    // ── Query execution ──────────────────────────────────────────────────────

    async fn execute_queries(&mut self, original_text: String, queries: Vec<String>) {
        // Echo query to output pane with ❯ prompt, one visual line per SQL line
        let mut lines = original_text.trim().lines();
        if let Some(first) = lines.next() {
            self.output.push_prompt(format!("❯ {}", first));
            for line in lines {
                self.output.push_prompt(format!("  {}", line));
            }
        }

        let (tx, rx) = mpsc::unbounded_channel::<TuiMsg>();
        let cancel_token = CancellationToken::new();

        self.query_rx = Some(rx);
        self.cancel_token = Some(cancel_token.clone());
        self.is_running = true;
        self.running_hint.clear();
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
        self.textarea = TextArea::new(if lines.is_empty() {
            vec![String::new()]
        } else {
            lines
        });
        // Move cursor to end of content
        self.textarea.move_cursor(CursorMove::Bottom);
        self.textarea.move_cursor(CursorMove::End);
    }

    // ── Rendering ────────────────────────────────────────────────────────────

    fn render(&mut self, f: &mut ratatui::Frame) {
        let area = f.area();

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
        }

        // Status bar
        self.render_status_bar(f, layout.status);
    }

    fn render_running_pane(&self, f: &mut ratatui::Frame, area: Rect) {
        let outer_block = Block::default()
            .borders(Borders::TOP | Borders::BOTTOM)
            .border_style(Style::default().fg(Color::DarkGray));
        let inner = outer_block.inner(area);
        f.render_widget(outer_block, area);

        let spinner = SPINNER_FRAMES[(self.spinner_tick / 2) as usize % SPINNER_FRAMES.len()];
        let elapsed = if let Some(start) = self.query_start {
            format!("{} {:.1}s", spinner, start.elapsed().as_secs_f64())
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

    fn render_status_bar(&self, f: &mut ratatui::Frame, area: Rect) {
        let host = &self.context.args.host;
        let db = &self.context.args.database;

        let left = format!(" {} | {} | v{}", host, db, CLI_VERSION);
        let right = if self.is_running {
            " Ctrl+C cancel ".to_string()
        } else {
            " Ctrl+D exit  Ctrl+H help ".to_string()
        };

        // Pad between left and right
        let total = area.width as usize;
        let pad = total.saturating_sub(left.len() + right.len());
        let status_text = format!("{}{}{}", left, " ".repeat(pad), right);

        let status = Paragraph::new(status_text)
            .style(Style::default().bg(Color::DarkGray).fg(Color::White));
        f.render_widget(status, area);
    }
}
