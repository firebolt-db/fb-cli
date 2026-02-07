pub mod context_detector;
pub mod schema_cache;

use context_detector::{detect_context, find_word_start};
use rustyline::completion::{Completer, Pair};
use rustyline::Context as RustylineContext;
use schema_cache::SchemaCache;
use std::sync::Arc;

pub struct SqlCompleter {
    cache: Arc<SchemaCache>,
    enabled: bool,
}

impl SqlCompleter {
    pub fn new(cache: Arc<SchemaCache>, enabled: bool) -> Self {
        Self { cache, enabled }
    }

    pub fn set_enabled(&mut self, enabled: bool) {
        self.enabled = enabled;
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn cache(&self) -> &Arc<SchemaCache> {
        &self.cache
    }
}

impl Completer for SqlCompleter {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &RustylineContext<'_>,
    ) -> rustyline::Result<(usize, Vec<Self::Candidate>)> {
        if !self.enabled {
            return Ok((0, Vec::new()));
        }

        // Find the start of the word we're completing
        let word_start = find_word_start(line, pos);
        let partial = &line[word_start..pos];

        // Return only tables and columns (no keywords)
        let mut candidates = Vec::new();

        // Add table names
        candidates.extend(self.cache.get_tables(partial));

        // Add column names
        candidates.extend(self.cache.get_columns(partial));

        // Convert to Pair format (display, replacement)
        let pairs: Vec<Pair> = candidates
            .into_iter()
            .map(|c| Pair {
                display: c.clone(),
                replacement: c,
            })
            .collect();

        Ok((word_start, pairs))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustyline::history::DefaultHistory;

    #[test]
    fn test_completer_disabled() {
        let cache = Arc::new(SchemaCache::new(300));
        let completer = SqlCompleter::new(cache, false);

        let history = DefaultHistory::new();
        let ctx = RustylineContext::new(&history);
        let result = completer.complete("SELECT * FROM us", 17, &ctx).unwrap();

        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn test_completer_no_keywords() {
        let cache = Arc::new(SchemaCache::new(300));
        let completer = SqlCompleter::new(cache, true);

        let history = DefaultHistory::new();
        let ctx = RustylineContext::new(&history);
        let result = completer.complete("SEL", 3, &ctx).unwrap();

        // Should not return keywords (only tables and columns)
        assert!(!result.1.iter().any(|p| p.display == "SELECT"));
    }
}
