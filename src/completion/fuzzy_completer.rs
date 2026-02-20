/// Fuzzy schema search across all tables, columns, and functions.
///
/// Used by the Ctrl+Space overlay in the TUI.  Collects all items from the
/// schema cache and applies nucleo fuzzy matching to rank them.
use nucleo_matcher::{
    pattern::{CaseMatching, Normalization, Pattern},
    Config, Matcher, Utf32Str,
};
use std::sync::Arc;

use super::{schema_cache::SchemaCache, usage_tracker::ItemType};

pub struct FuzzyCompleter {
    cache: Arc<SchemaCache>,
    matcher: Matcher,
}

impl FuzzyCompleter {
    pub fn new(cache: Arc<SchemaCache>) -> Self {
        Self {
            cache,
            matcher: Matcher::new(Config::DEFAULT),
        }
    }

    /// Return all schema items that fuzzy-match `query`, ranked by score then priority.
    /// Returns up to `limit` items.
    pub fn search(&mut self, query: &str, limit: usize) -> Vec<FuzzyItem> {
        let all_items = self.collect_all_items();

        if query.is_empty() {
            let mut items = all_items;
            items.sort_by(|a, b| {
                b.priority.cmp(&a.priority).then(a.label.cmp(&b.label))
            });
            items.truncate(limit);
            return items;
        }

        let pattern = Pattern::parse(query, CaseMatching::Ignore, Normalization::Smart);

        let mut scored: Vec<(u32, FuzzyItem)> = all_items
            .into_iter()
            .filter_map(|item| {
                let haystack = Utf32Str::Ascii(item.label.as_bytes());
                let score = pattern.score(haystack, &mut self.matcher);
                score.map(|s| (s, item))
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

    /// Collect all schema items (tables + columns + functions) into a flat list.
    fn collect_all_items(&self) -> Vec<FuzzyItem> {
        let mut items = Vec::new();

        // Tables
        for (schema, table) in self.cache.get_all_tables() {
            let is_public = schema == "public" || schema.is_empty();
            let label = if is_public {
                table.clone()
            } else {
                format!("{}.{}", schema, table)
            };
            items.push(FuzzyItem {
                insert_value: label.clone(),
                label,
                description: "table".to_string(),
                item_type: ItemType::Table,
                // Public tables rank above non-public tables.
                priority: if is_public { 3 } else { 2 },
            });
        }

        // Columns — always shown as table.column (or schema.table.column for non-public).
        for (schema, table, column) in self.cache.get_all_columns() {
            let is_public = schema == "public" || schema.is_empty();
            let label = if is_public {
                format!("{}.{}", table, column)
            } else {
                format!("{}.{}.{}", schema, table, column)
            };
            items.push(FuzzyItem {
                insert_value: label.clone(),
                label,
                description: "column".to_string(),
                item_type: ItemType::Column,
                priority: 1,
            });
        }

        // Functions
        for function in self.cache.get_all_functions() {
            items.push(FuzzyItem {
                label: format!("{}()", function),
                description: "function".to_string(),
                item_type: ItemType::Function,
                insert_value: format!("{}(", function),
                priority: 0,
            });
        }

        items
    }
}

/// A single item returned by the fuzzy search.
#[derive(Clone, Debug)]
pub struct FuzzyItem {
    /// Displayed text in the result list.
    pub label: String,
    /// Short description matching tab-completion labels: "table" / "column" / "function".
    pub description: String,
    /// The logical type.
    pub item_type: ItemType,
    /// Text to insert into the textarea when accepted.
    pub insert_value: String,
    /// Priority tier used as a tiebreaker when fuzzy scores are equal.
    /// Higher = shown first. Public tables=3, non-public tables=2, columns=1, functions=0.
    pub priority: u32,
}
