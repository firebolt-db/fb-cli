/// Floating tab-completion popup rendered above the input pane.
///
/// The popup is a bordered `List` widget positioned so that the left edge
/// aligns with the first character of the word being completed.
use ratatui::{
    layout::Rect,
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{Block, Borders, Clear, List, ListItem},
};

use crate::completion::CompletionItem;

/// State for an open completion popup session.
pub struct CompletionState {
    /// All available completion items.
    pub items: Vec<CompletionItem>,
    /// Index of the currently highlighted item.
    pub selected: usize,
    /// Index of the first visible item (scroll position).
    pub scroll_offset: usize,
    /// Byte offset in the full textarea content where the partial word starts.
    pub word_start_byte: usize,
    /// Visual column (0-based) of the word start within its line.
    /// Used for popup positioning.
    pub word_start_col: usize,
    /// Textarea cursor row (0-based) when completion was triggered.
    pub cursor_row: usize,
}

impl CompletionState {
    pub fn new(
        items: Vec<CompletionItem>,
        word_start_byte: usize,
        word_start_col: usize,
        cursor_row: usize,
    ) -> Self {
        Self {
            items,
            selected: 0,
            scroll_offset: 0,
            word_start_byte,
            word_start_col,
            cursor_row,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn selected_item(&self) -> Option<&CompletionItem> {
        self.items.get(self.selected)
    }

    /// Advance selection to the next item (wrapping), scrolling the viewport.
    pub fn next(&mut self) {
        if !self.items.is_empty() {
            self.selected = (self.selected + 1) % self.items.len();
            self.ensure_visible();
        }
    }

    /// Move selection to the previous item (wrapping), scrolling the viewport.
    pub fn prev(&mut self) {
        if !self.items.is_empty() {
            self.selected = self.selected.checked_sub(1).unwrap_or(self.items.len() - 1);
            self.ensure_visible();
        }
    }

    /// Adjust scroll_offset so that `selected` is within the visible window.
    fn ensure_visible(&mut self) {
        let vis = MAX_VISIBLE as usize;
        if self.selected < self.scroll_offset {
            self.scroll_offset = self.selected;
        } else if self.selected >= self.scroll_offset + vis {
            self.scroll_offset = self.selected - vis + 1;
        }
    }
}

// ── Layout helpers ───────────────────────────────────────────────────────────

const MAX_VISIBLE: u16 = 10;
const DESCRIPTION_WIDTH: u16 = 10; // "table" / "column" / "function" / "schema"
const MIN_VALUE_WIDTH: u16 = 16;
const BORDER_OVERHEAD: u16 = 2; // left + right border

/// Compute the area for the completion popup.
///
/// `input_area`  — the full input pane rect (borders included).
/// `textarea_x`  — x coordinate of the left edge of the textarea column
///                 (i.e. after the "❯ " prompt columns).
/// `total`       — the full terminal area, for clamping.
pub fn popup_area(
    state: &CompletionState,
    input_area: Rect,
    textarea_x: u16,
    total: Rect,
) -> Rect {
    let visible = (state.items.len() as u16).min(MAX_VISIBLE);
    let popup_h = visible + BORDER_OVERHEAD;

    // Compute preferred width from longest item name
    let max_name = state
        .items
        .iter()
        .map(|i| i.value.len() as u16)
        .max()
        .unwrap_or(MIN_VALUE_WIDTH);
    let popup_w = (max_name + DESCRIPTION_WIDTH + BORDER_OVERHEAD + 2)
        .max(MIN_VALUE_WIDTH + DESCRIPTION_WIDTH + BORDER_OVERHEAD + 2)
        .min(total.width.saturating_sub(2));

    // Popup x: align with the word start column
    let preferred_x = textarea_x + state.word_start_col as u16;
    let x = if preferred_x + popup_w > total.width {
        total.width.saturating_sub(popup_w)
    } else {
        preferred_x
    };

    // Popup y: just above the input pane (in the output area)
    let y = input_area.y.saturating_sub(popup_h);

    Rect::new(x, y.max(0), popup_w, popup_h)
}

// ── Rendering ────────────────────────────────────────────────────────────────

/// Render the completion popup to the frame.
pub fn render(state: &CompletionState, area: Rect, f: &mut ratatui::Frame) {
    // Clear the background so the popup appears floating
    f.render_widget(Clear, area);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(Color::Cyan));

    let inner = block.inner(area);
    let value_width = inner.width.saturating_sub(DESCRIPTION_WIDTH) as usize;

    // Build list items — only the visible window starting at scroll_offset
    let items: Vec<ListItem> = state
        .items
        .iter()
        .enumerate()
        .skip(state.scroll_offset)
        .take(inner.height as usize)
        .map(|(idx, item)| {
            let is_selected = idx == state.selected;

            // Left: item value (truncated if needed)
            let name: String = item.value.chars().take(value_width).collect();
            let pad = value_width.saturating_sub(name.len());
            let padded_name = format!("{}{}", name, " ".repeat(pad));

            // Right: description (right-aligned in a fixed-width column)
            let desc: String = item
                .description
                .chars()
                .take(DESCRIPTION_WIDTH as usize - 1)
                .collect();
            let desc_pad = (DESCRIPTION_WIDTH as usize - 1).saturating_sub(desc.len());
            let padded_desc = format!("{}{} ", " ".repeat(desc_pad), desc);

            let (name_style, desc_style) = if is_selected {
                (
                    Style::default()
                        .fg(Color::Black)
                        .bg(Color::Cyan)
                        .add_modifier(Modifier::BOLD),
                    Style::default().fg(Color::Black).bg(Color::Cyan),
                )
            } else {
                (
                    Style::default().fg(Color::White),
                    Style::default().fg(Color::DarkGray),
                )
            };

            let line = Line::from(vec![
                Span::styled(padded_name, name_style),
                Span::styled(padded_desc, desc_style),
            ]);
            ListItem::new(line)
        })
        .collect();

    let list = List::new(items);
    f.render_widget(block, area);
    f.render_widget(list, inner);
}
