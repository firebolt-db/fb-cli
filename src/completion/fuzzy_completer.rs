/// Fuzzy schema search across all tables, columns, and functions.
///
/// Used by the Ctrl+Space overlay in the TUI.  Collects all items from the
/// schema cache via the shared `collect_candidates()` function and applies
/// nucleo fuzzy matching to rank them.
use nucleo_matcher::{
    pattern::{CaseMatching, Normalization, Pattern},
    Config, Matcher, Utf32Str,
};
use std::sync::Arc;

use super::candidates::{self, collect_candidates};
use super::schema_cache::SchemaCache;
use super::usage_tracker::UsageTracker;

pub struct FuzzyCompleter {
    cache: Arc<SchemaCache>,
    usage_tracker: Arc<UsageTracker>,
    matcher: Matcher,
}

impl FuzzyCompleter {
    pub fn new(cache: Arc<SchemaCache>, usage_tracker: Arc<UsageTracker>) -> Self {
        Self {
            cache,
            usage_tracker,
            matcher: Matcher::new(Config::DEFAULT),
        }
    }

    /// Return all schema items that fuzzy-match `query`, ranked by score then priority.
    ///
    /// `tables_in_query` — tables extracted from the current SQL statement
    /// (via `ContextAnalyzer::extract_tables`). Used to give context-aware
    /// priority boosts to columns whose tables are already referenced, matching
    /// the behaviour of tab completion.  Pass `&[]` when no context is available.
    ///
    /// Returns up to `limit` items.
    pub fn search(&mut self, query: &str, limit: usize, tables_in_query: &[String]) -> Vec<FuzzyItem> {
        let all = collect_candidates(&self.cache, self.usage_tracker.clone(), tables_in_query);

        if query.is_empty() {
            let mut items: Vec<FuzzyItem> = all.into_iter().map(candidate_to_fuzzy).collect();
            items.sort_by(|a, b| {
                b.priority
                    .cmp(&a.priority)
                    .then(a.label.cmp(&b.label))
            });
            items.truncate(limit);
            return items;
        }

        let pattern = Pattern::parse(query, CaseMatching::Ignore, Normalization::Smart);

        let mut scored: Vec<(u32, FuzzyItem)> = all
            .into_iter()
            .filter_map(|item| {
                let fi = candidate_to_fuzzy(item);
                // Run fuzzy match against the display label
                let haystack = Utf32Str::Ascii(fi.label.as_bytes());
                pattern
                    .score(haystack, &mut self.matcher)
                    .map(|s| (s, fi))
            })
            .collect();

        scored.sort_by(|a, b| {
            b.0.cmp(&a.0)
                .then(b.1.priority.cmp(&a.1.priority))
                .then(a.1.label.cmp(&b.1.label))
        });
        scored.truncate(limit);
        scored.into_iter().map(|(_, item)| item).collect()
    }
}

fn candidate_to_fuzzy(c: candidates::Candidate) -> FuzzyItem {
    FuzzyItem {
        label: c.display,
        description: c.description.to_string(),
        insert_value: c.insert,
        priority: c.priority,
    }
}

/// A single item returned by the fuzzy search.
#[derive(Clone, Debug)]
pub struct FuzzyItem {
    /// Displayed text in the result list.
    pub label: String,
    /// Short description matching tab-completion labels: "table" / "column" / "function" / "schema".
    pub description: String,
    /// Text to insert into the textarea when accepted.
    pub insert_value: String,
    /// Priority tier used as a tiebreaker when fuzzy scores are equal.
    /// Higher = shown first. Derived from the shared priority constants.
    pub priority: u32,
}
