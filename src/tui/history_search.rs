/// Incremental reverse-history-search state (Ctrl+R).
///
/// Tracks all matching history entries for the current query so the popup
/// can display a navigable list.
pub const MAX_VISIBLE: usize = 12;

pub struct HistorySearch {
    /// The characters the user has typed so far.
    query: String,
    /// Indices into `History::entries` of all matching entries, most-recent-first.
    all_matches: Vec<usize>,
    /// Which match is currently highlighted (index into `all_matches`).
    selected: usize,
    /// First visible row in the popup list.
    scroll_offset: usize,
    /// Text that was in the textarea when search began; restored on Escape.
    saved_content: String,
}

impl HistorySearch {
    pub fn new(saved_content: String) -> Self {
        Self {
            query: String::new(),
            all_matches: Vec::new(),
            selected: 0,
            scroll_offset: 0,
            saved_content,
        }
    }

    pub fn query(&self) -> &str {
        &self.query
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
        self.all_matches = (0..entries.len())
            .rev() // most-recent first
            .filter(|&i| q.is_empty() || entries[i].to_lowercase().contains(&q))
            .collect();
        self.selected = 0;
        self.scroll_offset = 0;
    }

    fn ensure_visible(&mut self) {
        if self.selected < self.scroll_offset {
            self.scroll_offset = self.selected;
        } else if self.selected >= self.scroll_offset + MAX_VISIBLE {
            self.scroll_offset = self.selected - MAX_VISIBLE + 1;
        }
    }

    // ── Public mutation ──────────────────────────────────────────────────────

    pub fn push_char(&mut self, c: char, entries: &[String]) {
        self.query.push(c);
        self.recompute(entries);
    }

    pub fn pop_char(&mut self, entries: &[String]) {
        self.query.pop();
        self.recompute(entries);
    }

    /// Move selection to the next older match.
    pub fn select_next(&mut self) {
        if self.selected + 1 < self.all_matches.len() {
            self.selected += 1;
            self.ensure_visible();
        }
    }

    /// Move selection to the next newer match.
    pub fn select_prev(&mut self) {
        if self.selected > 0 {
            self.selected -= 1;
            self.ensure_visible();
        }
    }

    /// Alias used by Ctrl+R (cycles to next older match).
    pub fn search_older(&mut self, _entries: &[String]) {
        self.select_next();
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
    fn test_empty_query_no_match_at_start() {
        let s = HistorySearch::new(String::new());
        let e = entries();
        // Nothing typed → no match yet
        assert!(s.matched(&e).is_none());
    }

    #[test]
    fn test_empty_query_after_pop_matches_most_recent() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        s.push_char('x', &e);
        s.pop_char(&e);
        // Empty query → all entries match; most-recent (index 3 = INSERT) is first
        assert_eq!(s.matched(&e), Some("INSERT INTO orders VALUES (1);"));
    }

    #[test]
    fn test_push_narrows_match() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        // Most recent "sel" match: "SELECT * FROM users;" (index 2 in entries)
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
    }

    #[test]
    fn test_ctrl_r_cycles_older() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
        s.search_older(&e);
        assert_eq!(s.matched(&e), Some("SELECT * FROM orders;"));
        s.search_older(&e);
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
        // Already at last match — stays there
        s.search_older(&e);
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
    }

    #[test]
    fn test_saved_content() {
        let s = HistorySearch::new("my draft query".to_string());
        assert_eq!(s.saved_content(), "my draft query");
    }

    #[test]
    fn test_all_matches_count() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        // "sel" matches 3 SELECT entries
        assert_eq!(s.all_matches().len(), 3);
    }
}
