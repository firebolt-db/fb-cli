/// Incremental reverse-history-search state (Ctrl+R).
///
/// The search is case-insensitive substring match, walking backwards
/// through the history list (most-recent-first).  Successive Ctrl+R presses
/// cycle to older matches.
pub struct HistorySearch {
    /// The characters the user has typed so far.
    query: String,
    /// Index into `History::entries` of the current match (None = no match).
    matched_idx: Option<usize>,
    /// Text that was in the textarea when search began; restored on Escape.
    saved_content: String,
}

impl HistorySearch {
    pub fn new(saved_content: String) -> Self {
        Self {
            query: String::new(),
            matched_idx: None,
            saved_content,
        }
    }

    pub fn query(&self) -> &str {
        &self.query
    }

    /// The matched history entry, if any.
    pub fn matched<'a>(&self, entries: &'a [String]) -> Option<&'a str> {
        self.matched_idx.map(|i| entries[i].as_str())
    }

    /// The textarea content to restore when the user presses Escape.
    pub fn saved_content(&self) -> &str {
        &self.saved_content
    }

    // ── Search helpers ───────────────────────────────────────────────────────

    /// Search backwards through `entries[..before]` for an entry containing
    /// `query_lower`.  Returns the index of the first (most-recent) hit.
    fn search_before(query_lower: &str, entries: &[String], before: usize) -> Option<usize> {
        let end = before.min(entries.len());
        if query_lower.is_empty() {
            // No query → match the most-recent entry unconditionally.
            return if end > 0 { Some(end - 1) } else { None };
        }
        (0..end).rev().find(|&i| entries[i].to_lowercase().contains(query_lower))
    }

    // ── Public mutation ──────────────────────────────────────────────────────

    /// Append `c` to the query and re-search from the most-recent entry.
    pub fn push_char(&mut self, c: char, entries: &[String]) {
        self.query.push(c);
        let q = self.query.to_lowercase();
        // Re-search from the current match position (or end of list) so that
        // adding a character narrows the match rather than jumping further back.
        let before = self.matched_idx.map(|i| i + 1).unwrap_or(entries.len());
        self.matched_idx = Self::search_before(&q, entries, before);
    }

    /// Remove the last character from the query and re-search from the end.
    pub fn pop_char(&mut self, entries: &[String]) {
        self.query.pop();
        let q = self.query.to_lowercase();
        // After narrowing the query the most-recent match might have moved
        // forward, so restart from the end of the list.
        self.matched_idx = Self::search_before(&q, entries, entries.len());
    }

    /// Cycle to the next older match (invoked by a second Ctrl+R).
    pub fn search_older(&mut self, entries: &[String]) {
        let q = self.query.to_lowercase();
        // Search strictly before the current match.
        let before = self.matched_idx.unwrap_or(entries.len());
        self.matched_idx = Self::search_before(&q, entries, before);
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
    fn test_empty_query_matches_most_recent() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        // Without typing anything, matched_idx should be None (not started yet).
        assert!(s.matched(&e).is_none());
        // After pushing then deleting (empty query), most-recent entry returned.
        s.push_char('x', &e);
        s.pop_char(&e);
        assert_eq!(s.matched(&e), Some("INSERT INTO orders VALUES (1);"));
    }

    #[test]
    fn test_push_narrows_match() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        // Most recent "sel" match should be index 3 (INSERT has no sel),
        // index 2 "SELECT * FROM users;" → yes
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
    }

    #[test]
    fn test_ctrl_r_cycles_older() {
        let mut s = HistorySearch::new(String::new());
        let e = entries();
        // "sel" narrows to SELECT entries only (INSERT doesn't contain "sel")
        s.push_char('s', &e);
        s.push_char('e', &e);
        s.push_char('l', &e);
        // First match: most-recent SELECT entry
        assert_eq!(s.matched(&e), Some("SELECT * FROM users;"));
        s.search_older(&e);
        // Next older: "SELECT * FROM orders;" (index 1)
        assert_eq!(s.matched(&e), Some("SELECT * FROM orders;"));
        s.search_older(&e);
        // Next older: "SELECT 1;" (index 0)
        assert_eq!(s.matched(&e), Some("SELECT 1;"));
        s.search_older(&e);
        // No more matches — stays at None
        assert!(s.matched(&e).is_none());
    }

    #[test]
    fn test_saved_content() {
        let s = HistorySearch::new("my draft query".to_string());
        assert_eq!(s.saved_content(), "my draft query");
    }
}
