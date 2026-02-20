/// Ctrl+Space fuzzy schema search overlay.
///
/// A full-width popup that covers the input pane and lower part of the output
/// pane.  The user types a search query and sees ranked schema items in real time.
use ratatui::{
    layout::{Constraint, Layout, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem, Paragraph},
};

use crate::completion::fuzzy_completer::FuzzyItem;

const MAX_VISIBLE: u16 = 12;
const POPUP_HEIGHT: u16 = MAX_VISIBLE + 4; // list + search line + 2 borders

/// State for an open fuzzy-search session.
pub struct FuzzyState {
    /// Current search query typed by the user.
    pub query: String,
    /// Ranked items from the fuzzy completer (refreshed on each query change).
    pub items: Vec<FuzzyItem>,
    /// Currently highlighted item index.
    pub selected: usize,
}

impl FuzzyState {
    pub fn new() -> Self {
        Self {
            query: String::new(),
            items: Vec::new(),
            selected: 0,
        }
    }

    pub fn push_char(&mut self, c: char) {
        self.query.push(c);
        self.selected = 0;
    }

    pub fn pop_char(&mut self) {
        self.query.pop();
        self.selected = 0;
    }

    pub fn next(&mut self) {
        if !self.items.is_empty() {
            self.selected = (self.selected + 1) % self.items.len();
        }
    }

    pub fn prev(&mut self) {
        if !self.items.is_empty() {
            self.selected = self.selected.checked_sub(1).unwrap_or(self.items.len() - 1);
        }
    }

    pub fn selected_item(&self) -> Option<&FuzzyItem> {
        self.items.get(self.selected)
    }
}

/// Compute the popup rect: full-width, anchored just above the input pane.
pub fn popup_area(input_area: Rect, total: Rect) -> Rect {
    let h = POPUP_HEIGHT.min(input_area.y); // can't go above y=0
    let y = input_area.y.saturating_sub(h);
    Rect::new(0, y, total.width, h)
}

/// Render the fuzzy popup to the frame.
pub fn render(state: &FuzzyState, area: Rect, f: &mut ratatui::Frame) {
    f.render_widget(Clear, area);

    let block = Block::default()
        .title(" Fuzzy Schema Search  (Enter accept · Esc close) ")
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Magenta));

    let inner = block.inner(area);
    f.render_widget(block, area);

    // Split inner: search line at top, results below
    let chunks = Layout::vertical([
        Constraint::Length(1), // search input line
        Constraint::Min(0),    // results list
    ])
    .split(inner);

    // ── Search line ────────────────────────────────────────────────────────
    let search_line = Line::from(vec![
        Span::styled("/ ", Style::default().fg(Color::Magenta).add_modifier(Modifier::BOLD)),
        Span::styled(state.query.clone(), Style::default().fg(Color::White)),
        Span::styled("█", Style::default().fg(Color::Magenta)), // cursor indicator
    ]);
    f.render_widget(Paragraph::new(search_line), chunks[0]);

    // ── Results list ───────────────────────────────────────────────────────
    let desc_w = 16usize;
    let label_w = chunks[1].width as usize - desc_w - 1;

    let items: Vec<ListItem> = state
        .items
        .iter()
        .enumerate()
        .map(|(idx, item)| {
            let is_sel = idx == state.selected;

            // Truncate/pad label
            let label: String = item.label.chars().take(label_w).collect();
            let pad = label_w.saturating_sub(label.len());
            let padded = format!("{}{}", label, " ".repeat(pad));

            // Right-aligned description
            let desc: String = item.description.chars().take(desc_w - 1).collect();
            let dpd = format!("{:>width$}", desc, width = desc_w - 1);

            let (l_style, d_style) = if is_sel {
                (
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Magenta)
                        .add_modifier(Modifier::BOLD),
                    Style::default().fg(Color::Black).bg(Color::Magenta),
                )
            } else {
                (
                    Style::default().fg(Color::White),
                    Style::default().fg(Color::DarkGray),
                )
            };

            let line = Line::from(vec![
                Span::styled(padded, l_style),
                Span::styled(" ", if is_sel { l_style } else { Style::default() }),
                Span::styled(dpd, d_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    f.render_widget(List::new(items), chunks[1]);
}
