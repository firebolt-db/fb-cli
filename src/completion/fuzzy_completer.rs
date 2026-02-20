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

    /// Return all schema items that fuzzy-match `query`, ranked by score.
    /// Returns up to `limit` items.
    pub fn search(&mut self, query: &str, limit: usize) -> Vec<FuzzyItem> {
        let all_items = self.collect_all_items();

        if query.is_empty() {
            // Show first `limit` items sorted alphabetically
            let mut items = all_items;
            items.sort_by(|a, b| a.label.cmp(&b.label));
            items.truncate(limit);
            return items;
        }

        let pattern = Pattern::parse(
            query,
            CaseMatching::Ignore,
            Normalization::Smart,
        );

        // Score every item and keep matches with score > 0
        let mut scored: Vec<(u32, FuzzyItem)> = all_items
            .into_iter()
            .filter_map(|item| {
                let haystack = Utf32Str::Ascii(item.label.as_bytes());
                let score = pattern.score(haystack, &mut self.matcher);
                score.map(|s| (s, item))
            })
            .collect();

        // Sort by score descending
        scored.sort_by(|a, b| b.0.cmp(&a.0));
        scored.truncate(limit);
        scored.into_iter().map(|(_, item)| item).collect()
    }

    /// Collect all schema items (tables + columns + functions) into a flat list.
    fn collect_all_items(&self) -> Vec<FuzzyItem> {
        let mut items = Vec::new();

        // All tables (with schema prefix for non-public)
        for (schema, table) in self.cache.get_all_tables() {
            let label = if schema == "public" {
                table.clone()
            } else {
                format!("{}.{}", schema, table)
            };
            items.push(FuzzyItem {
                label,
                description: format!("table ({})", schema),
                item_type: ItemType::Table,
                insert_value: if schema == "public" {
                    table
                } else {
                    format!("{}.{}", schema, table)
                },
            });
        }

        // All columns (unqualified; qualified duplicates would be too noisy)
        for (table, column) in self.cache.get_all_columns() {
            items.push(FuzzyItem {
                label: format!("{}.{}", table, column),
                description: format!("column of {}", table),
                item_type: ItemType::Column,
                insert_value: column,
            });
        }

        // All functions
        for function in self.cache.get_all_functions() {
            items.push(FuzzyItem {
                label: format!("{}()", function),
                description: "function".to_string(),
                item_type: ItemType::Function,
                insert_value: format!("{}(", function),
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
    /// Short description (type, schema).
    pub description: String,
    /// The logical type.
    pub item_type: ItemType,
    /// Text to insert into the textarea when accepted.
    pub insert_value: String,
}
