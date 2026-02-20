use std::fs;
use std::io::Write;
use std::path::PathBuf;

/// File-backed history with cursor-based navigation.
/// Entries are stored one per line; embedded newlines are escaped as `\n`.
pub struct History {
    entries: Vec<String>,
    /// None = not currently navigating. Some(i) = showing entries[i].
    cursor: Option<usize>,
    /// The text that was in the editor when navigation started.
    saved_current: String,
    path: PathBuf,
}

impl History {
    pub fn new(path: PathBuf) -> Self {
        Self {
            entries: Vec::new(),
            cursor: None,
            saved_current: String::new(),
            path,
        }
    }

    /// Load history from disk. Each line is one entry; `\n` in entries is stored as `\\n`.
    pub fn load(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        if !self.path.exists() {
            return Ok(());
        }
        let content = fs::read_to_string(&self.path)?;
        self.entries = content
            .lines()
            .filter(|l| !l.trim().is_empty())
            .map(|l| l.replace("\\n", "\n"))
            .collect();
        // Keep at most 10 000 entries (trim oldest)
        if self.entries.len() > 10_000 {
            let start = self.entries.len() - 10_000;
            self.entries = self.entries[start..].to_vec();
        }
        Ok(())
    }

    /// Append a single entry to the history file without rewriting the whole file.
    fn append_to_file(&self, entry: &str) -> Result<(), Box<dyn std::error::Error>> {
        let mut file = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let stored = entry.replace('\n', "\\n");
        writeln!(file, "{}", stored)?;
        Ok(())
    }

    /// Add a new entry. Deduplicates consecutive identical entries.
    pub fn add(&mut self, entry: String) {
        if entry.trim().is_empty() {
            return;
        }
        if self.entries.last().map(|e| e == &entry).unwrap_or(false) {
            self.cursor = None;
            return;
        }
        self.entries.push(entry.clone());
        if self.entries.len() > 10_000 {
            self.entries.remove(0);
        }
        self.cursor = None;
        let _ = self.append_to_file(&entry);
    }

    /// Read-only view of all history entries (oldest first).
    pub fn entries(&self) -> &[String] {
        &self.entries
    }

    /// Navigate backward (older). Returns the entry to display, or `None` if history is empty.
    /// `current` is the text currently in the editor (saved so we can restore it on forward).
    pub fn go_back(&mut self, current: &str) -> Option<String> {
        if self.entries.is_empty() {
            return None;
        }
        match self.cursor {
            None => {
                self.saved_current = current.to_string();
                self.cursor = Some(self.entries.len() - 1);
            }
            Some(0) => {} // already at oldest – stay
            Some(i) => {
                self.cursor = Some(i - 1);
            }
        }
        self.cursor.map(|i| self.entries[i].clone())
    }

    /// Navigate forward (newer). Returns the entry to display, or the saved current text
    /// when we return past the newest entry.
    pub fn go_forward(&mut self) -> Option<String> {
        match self.cursor {
            None => None,
            Some(i) if i + 1 >= self.entries.len() => {
                self.cursor = None;
                Some(self.saved_current.clone())
            }
            Some(i) => {
                self.cursor = Some(i + 1);
                Some(self.entries[i + 1].clone())
            }
        }
    }

    /// Reset navigation state (called when user types something new).
    pub fn reset_navigation(&mut self) {
        self.cursor = None;
        self.saved_current.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn dummy() -> History {
        History::new(PathBuf::from("/tmp/test_fb_history_dummy"))
    }

    #[test]
    fn test_add_and_navigate() {
        let mut h = dummy();
        h.add("SELECT 1;".to_string());
        h.add("SELECT 2;".to_string());
        h.add("SELECT 3;".to_string());

        assert_eq!(h.go_back(""), Some("SELECT 3;".to_string()));
        assert_eq!(h.go_back(""), Some("SELECT 2;".to_string()));
        assert_eq!(h.go_back(""), Some("SELECT 1;".to_string()));
        // Already at oldest – stays
        assert_eq!(h.go_back(""), Some("SELECT 1;".to_string()));

        // Forward brings us back
        assert_eq!(h.go_forward(), Some("SELECT 2;".to_string()));
        assert_eq!(h.go_forward(), Some("SELECT 3;".to_string()));
        // Past newest – returns saved current
        assert_eq!(h.go_forward(), Some("".to_string()));
    }

    #[test]
    fn test_dedup_consecutive() {
        let mut h = dummy();
        h.add("SELECT 1;".to_string());
        h.add("SELECT 1;".to_string());
        assert_eq!(h.entries.len(), 1);
    }

    #[test]
    fn test_reset_navigation() {
        let mut h = dummy();
        h.add("a".to_string());
        h.add("b".to_string());
        h.go_back("current");
        h.reset_navigation();
        assert!(h.cursor.is_none());
    }
}
