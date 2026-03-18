/// Incremental reverse-history-search state (Ctrl+R).
///
/// Displayed as a bottom-up popup: the search box is at the bottom and
/// matches are listed above it with the most-recent entry at the bottom.
pub const MAX_VISIBLE: usize = 16;

pub struct HistorySearch {
    /// The characters the user has typed so far.
    query: String,
    /// Byte offset of the insertion cursor within `query`.
    cursor_pos: usize,
    /// Indices into `History::entries` of all matching entries, most-recent-first.
    all_matches: Vec<usize>,
    /// Which match is currently highlighted (index into `all_matches`, 0 = most recent).
    selected: usize,
    /// Scroll offset: index of the entry shown at the bottom of the visible window.
    scroll_offset: usize,
    /// Text that was in the textarea when search began; restored on Escape.
    saved_content: String,
}

impl HistorySearch {
    /// Create a new search session.  Immediately populates all matches so that
    /// the popup shows recent history before the user types anything.
    pub fn new(saved_content: String, entries: &[String]) -> Self {
        let mut s = Self {
            query: String::new(),
            cursor_pos: 0,
            all_matches: Vec::new(),
            selected: 0,
            scroll_offset: 0,
            saved_content,
        };
        s.recompute(entries);
        s
    }

    #[allow(dead_code)]
    pub fn query(&self) -> &str {
        &self.query
    }

    /// The part of the query before the cursor (for rendering).
    pub fn query_before_cursor(&self) -> &str {
        &self.query[..self.cursor_pos]
    }

    /// The part of the query at and after the cursor (for rendering).
    pub fn query_after_cursor(&self) -> &str {
        &self.query[self.cursor_pos..]
    }

    pub fn all_matches(&self) -> &[usize] {
        &self.all_matches
    }

    pub fn selected(&self) -> usize {
        self.selected
    }

    pub fn scroll_offset(&self) -> usize {
        self.scroll_offset
    }

    /// The currently highlighted history entry, if any.
    pub fn matched<'a>(&self, entries: &'a [String]) -> Option<&'a str> {
        self.all_matches.get(self.selected).map(|&i| entries[i].as_str())
    }

    /// The textarea content to restore when the user presses Escape.
    pub fn saved_content(&self) -> &str {
        &self.saved_content
    }

    // ── Private helpers ──────────────────────────────────────────────────────

    fn recompute(&mut self, entries: &[String]) {
        let q = self.query.to_lowercase();
        let mut result = Vec::new();
        let mut last_matched = "";
        for i in (0..entries.len()).rev() {
            let entry = entries[i].as_str();
            if q.is_empty() || entry.to_lowercase().contains(&q) {
                // Skip only if identical to the previous entry that also matched —
                // i.e. consecutive duplicates within the filtered result list.
                if entry != last_matched {
                    result.push(i);
                }
                last_matched = entry;
            }
        }
        self.all_matches = result;
        self.selected = 0;
        self.scroll_offset = 0;
    }

    /// Keep `selected` within the visible scroll window.
    fn ensure_visible(&mut self, visible_rows: usize) {
        let h = visible_rows.max(1);
        if self.selected < self.scroll_offset {
            self.scroll_offset = self.selected;
        } else if self.selected >= self.scroll_offset + h {
            self.scroll_offset = self.selected - h + 1;
        }
    }

    // ── Public mutation ──────────────────────────────────────────────────────

    pub fn push_char(&mut self, c: char, entries: &[String]) {
        self.query.insert(self.cursor_pos, c);
        self.cursor_pos += c.len_utf8();
        self.recompute(entries);
    }

    pub fn pop_char(&mut self, entries: &[String]) {
        if self.cursor_pos == 0 {
            return;
        }
        // Find start of the character just before cursor_pos.
        let new_pos = self.query[..self.cursor_pos]
            .char_indices()
            .next_back()
            .map(|(i, _)| i)
            .unwrap_or(0);
        self.query.remove(new_pos);
        self.cursor_pos = new_pos;
        self.recompute(entries);
    }

    /// Move the cursor one character to the left.
    pub fn move_cursor_left(&mut self) {
        if self.cursor_pos == 0 {
            return;
        }
        self.cursor_pos = self.query[..self.cursor_pos]
            .char_indices()
            .next_back()
            .map(|(i, _)| i)
            .unwrap_or(0);
    }

    /// Move the cursor one character to the right.
    pub fn move_cursor_right(&mut self) {
        if self.cursor_pos < self.query.len() {
            let ch = self.query[self.cursor_pos..].chars().next().unwrap();
            self.cursor_pos += ch.len_utf8();
        }
    }

    /// Move the cursor to the start of the query (Ctrl+A).
    pub fn cursor_to_start(&mut self) {
        self.cursor_pos = 0;
    }

    /// Move selection to the next older match (Up arrow / Ctrl+R).
    /// `visible_rows` is the height of the list area for scroll tracking.
    pub fn select_older(&mut self, visible_rows: usize) {
        if self.selected + 1 < self.all_matches.len() {
            self.selected += 1;
            self.ensure_visible(visible_rows);
        }
    }

    /// Move selection to the next newer match (Down arrow).
    pub fn select_newer(&mut self, visible_rows: usize) {
        if self.selected > 0 {
            self.selected -= 1;
            self.ensure_visible(visible_rows);
        }
    }

    /// Alias for Ctrl+R (cycles to next older match).
    pub fn search_older(&mut self, _entries: &[String], visible_rows: usize) {
        self.select_older(visible_rows);
    }
}

/// Format a (possibly multi-line) history entry for single-line display.
/// Newlines are replaced with `↵` and the result is truncated with `(...)`.
pub fn format_entry_oneline(entry: &str, max_chars: usize) -> String {
    // Join lines with ↵ separator
    let joined: String = {
        let mut parts = entry.lines();
        let first = parts.next().unwrap_or("").trim_end();
        let mut s = first.to_string();
        for part in parts {
            let trimmed = part.trim();
            if !trimmed.is_empty() {
                s.push_str(" ↵ ");
                s.push_str(trimmed);
            }
        }
        s
    };

    if joined.chars().count() <= max_chars {
        joined
    } else {
        let suffix = " (...)";
        let take = max_chars.saturating_sub(suffix.chars().count());
        let truncated: String = joined.chars().take(take).collect();
        format!("{}{}", truncated, suffix)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entries() -> Vec<String> {
        vec![
            "SELECT 1;".to_string(),
            "SELECT * FROM orders;".to_string(),
            "SELECT * FROM users;".to_string(),
            "INSERT INTO orders VALUES (1);".to_string(),
        ]
    }

    #[test]
    fn test_new_shows_all_entries() {
        let s = HistorySearch::new(String::new(), &entries());
        let e = entries();
        // No query → all entries match; most-recent (INSERT) is first
        assert_eq!(s.all_matches().len(), 4);
        assert_eq!(s.matched(&e), Some("INSERT INTO orders VALUES (1);"));
    }

    #[test]
    fn test_push_narrows_match() {
        let mut s = HistorySearch::new(String::new(), &entries());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        assert_eq!(s.all_matches().len(), 3);
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
    }

    #[test]
    fn test_select_older_cycles() {
        let mut s = HistorySearch::new(String::new(), &entries());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
        s.select_older(100);
        assert_eq!(s.matched(&e), Some("SELECT * FROM orders;"));
        s.select_older(100);
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
        // At last match — stays there
        s.select_older(100);
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
    }

    #[test]
    fn test_select_newer() {
        let mut s = HistorySearch::new(String::new(), &entries());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        s.select_older(100);
        s.select_older(100);
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
        s.select_newer(100);
        assert_eq!(s.matched(&e), Some("SELECT * FROM orders;"));
    }

    #[test]
    fn test_saved_content() {
        let s = HistorySearch::new("my draft query".to_string(), &[]);
        assert_eq!(s.saved_content(), "my draft query");
    }

    #[test]
    fn test_format_entry_oneline_single() {
        assert_eq!(format_entry_oneline("SELECT 1;", 80), "SELECT 1;");
    }

    #[test]
    fn test_format_entry_oneline_multiline() {
        let s = format_entry_oneline("SELECT 1\nFROM t", 80);
        assert!(s.contains("↵"));
        assert!(s.contains("FROM t"));
    }

    #[test]
    fn test_format_entry_oneline_truncate() {
        let long = "SELECT very_long_column_name FROM some_table WHERE condition IS TRUE";
        let result = format_entry_oneline(long, 20);
        assert!(result.ends_with("(...)"));
        assert!(result.chars().count() <= 20);
    }

    #[test]
    fn test_consecutive_duplicates_in_results() {
        let e = vec![
            "SELECT 1;".to_string(),
            "SELECT 2;".to_string(),
            "SELECT 2;".to_string(), // consecutive with above → collapsed
            "SELECT 2;".to_string(), // consecutive → collapsed
            "SELECT 1;".to_string(), // not consecutive with SELECT 1 at index 0
                                     // because SELECT 2 entries are between them
        ];
        // No filter: result list is SELECT 1, SELECT 2, SELECT 2, SELECT 2, SELECT 1
        // (most-recent-first). After consecutive dedup in results: SELECT 1, SELECT 2, SELECT 1
        let s = HistorySearch::new(String::new(), &e);
        assert_eq!(s.all_matches().len(), 3);

        // Filter "2": only SELECT 2 entries match → all three are consecutive in
        // the result list → collapsed to one.
        let mut s2 = HistorySearch::new(String::new(), &e);
        s2.push_char('2', &e);
        assert_eq!(s2.all_matches().len(), 1);
    }
}
