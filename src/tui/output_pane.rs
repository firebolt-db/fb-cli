use crate::tui_msg::{TuiColor, TuiLine, TuiSpan};
use ratatui::{
    buffer::Buffer,
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Paragraph, Widget},
};

// ── OutputLine ────────────────────────────────────────────────────────────────

/// A single pre-rendered line in the output pane.
pub struct OutputLine {
    content: Line<'static>,
}

impl OutputLine {
    fn from_line(content: Line<'static>) -> Self {
        Self { content }
    }
}

// ── OutputPane ────────────────────────────────────────────────────────────────

/// Scrollable output pane that accumulates lines from query results.
pub struct OutputPane {
    lines: Vec<OutputLine>,
    /// Scroll offset in lines. `usize::MAX` is a sentinel meaning "scroll to bottom",
    /// resolved to the correct value by `clamp_scroll` before each render.
    scroll: usize,
}

impl OutputPane {
    pub fn new() -> Self {
        Self { lines: Vec::new(), scroll: 0 }
    }

    /// Push text that may contain ANSI SGR escape codes (e.g. from comfy-table).
    /// Splits on `\n` and parses each sub-line into styled spans.
    pub fn push_ansi_text(&mut self, text: &str) {
        for line in text.split('\n') {
            self.lines.push(OutputLine::from_line(parse_ansi_line(line)));
        }
        self.scroll_to_bottom();
    }

    /// Push a plain unstyled line.
    pub fn push_line(&mut self, line: impl Into<String>) {
        let s: String = line.into();
        self.lines.push(OutputLine::from_line(Line::from(s)));
        self.scroll_to_bottom();
    }

    /// Push the echoed prompt line (`❯ SELECT …`).
    /// The `❯ ` prefix is rendered in green+bold; the SQL text in yellow.
    /// Continuation lines (indented with spaces) are rendered fully in yellow.
    pub fn push_prompt(&mut self, line: impl Into<String>) {
        let s: String = line.into();
        let ratatui_line = if let Some(sql) = s.strip_prefix("❯ ") {
            Line::from(vec![
                Span::styled("❯ ", Style::default().fg(Color::Green).add_modifier(Modifier::BOLD)),
                Span::styled(sql.to_string(), Style::default().fg(Color::Yellow)),
            ])
        } else {
            Line::from(Span::styled(s, Style::default().fg(Color::Yellow)))
        };
        self.lines.push(OutputLine::from_line(ratatui_line));
        self.scroll_to_bottom();
    }

    /// Push a timing / scan-statistics line — dark gray.
    pub fn push_stat(&mut self, text: &str) {
        for line in text.split('\n') {
            let span = Span::styled(
                line.to_string(),
                Style::default().fg(Color::DarkGray),
            );
            self.lines.push(OutputLine::from_line(Line::from(span)));
        }
        self.scroll_to_bottom();
    }

    /// Push pre-styled lines produced by the TUI table renderer.
    pub fn push_tui_lines(&mut self, lines: Vec<TuiLine>) {
        for tui_line in lines {
            let spans: Vec<Span<'static>> = tui_line.0.into_iter().map(tui_span_to_ratatui).collect();
            self.lines.push(OutputLine::from_line(Line::from(spans)));
        }
        self.scroll_to_bottom();
    }

    /// Push an error line — red.
    pub fn push_error(&mut self, text: &str) {
        for line in text.split('\n') {
            let span = Span::styled(line.to_string(), Style::default().fg(Color::Red));
            self.lines.push(OutputLine::from_line(Line::from(span)));
        }
        self.scroll_to_bottom();
    }

    pub fn scroll_up(&mut self, amount: u16) {
        self.scroll = self.scroll.saturating_sub(amount as usize);
    }

    pub fn scroll_down(&mut self, amount: u16) {
        self.scroll = self.scroll.saturating_add(amount as usize);
    }

    fn scroll_to_bottom(&mut self) {
        self.scroll = usize::MAX; // sentinel — resolved in clamp_scroll
    }

    /// Clamp scroll so we don't go past the end of content.
    pub fn clamp_scroll(&mut self, visible_height: u16) {
        let total = self.lines.len();
        let height = visible_height as usize;
        if total <= height {
            self.scroll = 0;
        } else if self.scroll == usize::MAX || self.scroll > total - height {
            self.scroll = total - height;
        }
        // else: user-scrolled position is still in range, leave it alone
    }

    /// Render only the visible slice of lines — O(visible_height), not O(total).
    /// Content is bottom-anchored: empty padding fills the top so output
    /// grows upward from the input area, like a normal terminal.
    pub fn render(&self, area: Rect, buf: &mut Buffer) {
        let start = self.scroll;
        let end = (start + area.height as usize).min(self.lines.len());
        let height = area.height as usize;

        let content: Vec<Line> = self.lines[start..end]
            .iter()
            .map(|l| l.content.clone())
            .collect();

        // Pad with blank lines above so content sits at the bottom.
        let padding = height.saturating_sub(content.len());
        let visible: Vec<Line> = std::iter::repeat_n(Line::raw(""), padding)
            .chain(content)
            .collect();

        Widget::render(Paragraph::new(visible), area, buf);
    }
}

// ── TuiSpan → ratatui Span conversion ────────────────────────────────────────

fn tui_span_to_ratatui(span: TuiSpan) -> Span<'static> {
    let color = match span.color {
        TuiColor::Default  => Color::Reset,
        TuiColor::Cyan     => Color::Cyan,
        TuiColor::DarkGray => Color::DarkGray,
    };
    let mut style = Style::default().fg(color);
    if span.bold {
        style = style.add_modifier(Modifier::BOLD);
    }
    Span::styled(span.text, style)
}

// ── ANSI SGR parser ───────────────────────────────────────────────────────────

/// Parse a string that may contain ANSI SGR escape sequences (`\x1b[...m`) into
/// a ratatui `Line` of styled spans.  Unrecognised codes are silently ignored.
fn parse_ansi_line(text: &str) -> Line<'static> {
    let mut spans: Vec<Span<'static>> = Vec::new();
    let mut current_style = Style::default();
    let bytes = text.as_bytes();
    let mut seg_start = 0; // byte index of start of current unstyled segment
    let mut i = 0;

    while i < bytes.len() {
        // Look for ESC [
        if bytes[i] == 0x1b && i + 1 < bytes.len() && bytes[i + 1] == b'[' {
            // Flush the text segment before this escape sequence.
            if seg_start < i {
                spans.push(Span::styled(text[seg_start..i].to_string(), current_style));
            }
            // Scan forward to the terminating letter.
            let param_start = i + 2;
            let mut j = param_start;
            while j < bytes.len() && !bytes[j].is_ascii_alphabetic() {
                j += 1;
            }
            if j < bytes.len() && bytes[j] == b'm' {
                // SGR sequence — update style.
                let params = &text[param_start..j];
                current_style = apply_sgr(current_style, params);
                i = j + 1;
            } else {
                // Non-SGR or malformed — skip the ESC byte and retry.
                i += 1;
            }
            seg_start = i;
        } else {
            i += 1;
        }
    }

    // Flush any remaining text.
    if seg_start < text.len() {
        spans.push(Span::styled(text[seg_start..].to_string(), current_style));
    }

    Line::from(spans)
}

/// Apply a semicolon-separated list of SGR parameters to `style`.
fn apply_sgr(style: Style, params: &str) -> Style {
    // Empty params or "0" alone means reset.
    if params.is_empty() || params == "0" {
        return Style::default();
    }
    let mut s = style;
    for param in params.split(';') {
        s = match param {
            "0"  => Style::default(),
            "1"  => s.add_modifier(Modifier::BOLD),
            "2"  => s.add_modifier(Modifier::DIM),
            "3"  => s.add_modifier(Modifier::ITALIC),
            "4"  => s.add_modifier(Modifier::UNDERLINED),
            "22" => s.remove_modifier(Modifier::BOLD),
            "23" => s.remove_modifier(Modifier::ITALIC),
            "24" => s.remove_modifier(Modifier::UNDERLINED),
            // Standard foreground colours
            "30" => s.fg(Color::Black),
            "31" => s.fg(Color::Red),
            "32" => s.fg(Color::Green),
            "33" => s.fg(Color::Yellow),
            "34" => s.fg(Color::Blue),
            "35" => s.fg(Color::Magenta),
            "36" => s.fg(Color::Cyan),
            "37" => s.fg(Color::White),
            "39" => s.fg(Color::Reset),
            // Bright foreground colours
            "90" => s.fg(Color::DarkGray),
            "91" => s.fg(Color::LightRed),
            "92" => s.fg(Color::LightGreen),
            "93" => s.fg(Color::LightYellow),
            "94" => s.fg(Color::LightBlue),
            "95" => s.fg(Color::LightMagenta),
            "96" => s.fg(Color::LightCyan),
            "97" => s.fg(Color::White),
            _ => s, // ignore unrecognised params
        };
    }
    s
}
