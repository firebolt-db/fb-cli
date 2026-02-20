pub mod context_analyzer;
pub mod context_detector;
pub mod fuzzy_completer;
pub mod priority_scorer;
pub mod schema_cache;
pub mod usage_tracker;

use context_analyzer::ContextAnalyzer;
use context_detector::find_word_start;
use priority_scorer::{PriorityScorer, ScoredSuggestion};
use schema_cache::SchemaCache;
use usage_tracker::{ItemType, UsageTracker};
use std::sync::Arc;

/// A single completion candidate with display metadata.
#[derive(Debug, Clone)]
pub struct CompletionItem {
    /// The text to insert when accepted.
    pub value: String,
    /// Short description shown in the popup (type label or schema name).
    pub description: String,
    /// The logical type of the item.
    pub item_type: ItemType,
}

pub struct SqlCompleter {
    cache: Arc<SchemaCache>,
    usage_tracker: Arc<UsageTracker>,
    scorer: PriorityScorer,
    enabled: bool,
}

impl SqlCompleter {
    pub fn new(cache: Arc<SchemaCache>, usage_tracker: Arc<UsageTracker>, enabled: bool) -> Self {
        let scorer = PriorityScorer::new(usage_tracker.clone());
        Self {
            cache,
            usage_tracker,
            scorer,
            enabled,
        }
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

    /// Return a list of completion candidates for the given input line and cursor position.
    /// Returns `(word_start, items)` where `word_start` is the byte offset of the
    /// start of the word being completed.
    pub fn complete_at(&self, line: &str, pos: usize) -> (usize, Vec<CompletionItem>) {
        if !self.enabled {
            return (0, Vec::<CompletionItem>::new());
        }

        // Find the start of the word we're completing
        let word_start = find_word_start(line, pos);
        let partial = &line[word_start..pos];

        // Extract context: tables mentioned in current statement
        let tables_in_line = ContextAnalyzer::extract_tables(line);

        // Generate scored suggestions
        let mut scored: Vec<ScoredSuggestion> = Vec::new();

        // Check if user is typing "schema_name." to see tables from that schema
        let (schema_filter, table_prefix) = if let Some(dot_pos) = partial.rfind('.') {
            let schema_part = &partial[..dot_pos];
            let table_part = &partial[dot_pos + 1..];
            (Some(schema_part), table_part)
        } else {
            (None, partial)
        };

        if let Some(schema_name) = schema_filter {
            let tables_in_schema = self.cache.get_tables_in_schema(schema_name, table_prefix);

            for table in tables_in_schema {
                let qualified_name = format!("{}.{}", schema_name, table);
                let score = self.scorer.score(&table, ItemType::Table, &tables_in_line, None);

                scored.push(ScoredSuggestion {
                    name: qualified_name,
                    item_type: ItemType::Table,
                    score,
                });
            }
        } else {
            let table_metadata = self.cache.get_tables_with_schema(partial);
            let column_metadata = self.cache.get_columns_with_table(partial);
            let schemas = self.cache.get_schemas(partial);

            let partial_lower = partial.to_lowercase();

            let partial_matches_schema = !partial.contains('.') &&
                schemas.iter().any(|s| s.to_lowercase().starts_with(&partial_lower));

            for schema in &schemas {
                let schema_with_dot = format!("{}.", schema);

                if schema_with_dot.to_lowercase().starts_with(&partial_lower) {
                    let base_score = 4000u32;
                    let usage_count = self.usage_tracker.get_count(ItemType::Table, schema);
                    let usage_bonus = usage_count.min(99) * 10;
                    let score = base_score + usage_bonus;

                    scored.push(ScoredSuggestion {
                        name: schema_with_dot,
                        item_type: ItemType::Table,
                        score,
                    });
                }
            }

            for (schema, table) in table_metadata {
                let short_name = table.clone();
                let qualified_name = format!("{}.{}", schema, table);

                let score = self.scorer.score(&short_name, ItemType::Table, &tables_in_line, None);

                if short_name.to_lowercase().starts_with(&partial_lower) {
                    scored.push(ScoredSuggestion {
                        name: short_name.clone(),
                        item_type: ItemType::Table,
                        score,
                    });
                }

                if qualified_name.to_lowercase().starts_with(&partial_lower) &&
                   (schema != "public" || partial.contains('.')) &&
                   !partial_matches_schema {
                    scored.push(ScoredSuggestion {
                        name: qualified_name,
                        item_type: ItemType::Table,
                        score,
                    });
                }
            }

            for (table, column) in column_metadata {
                let short_name = column.clone();

                let is_qualifying_table = partial.contains('.') &&
                    table.as_ref().map_or(false, |t| partial.to_lowercase().starts_with(&format!("{}.", t.to_lowercase())));

                let score = self.scorer.score(
                    &short_name,
                    ItemType::Column,
                    &tables_in_line,
                    table.as_deref(),
                );

                if !is_qualifying_table && short_name.to_lowercase().starts_with(&partial_lower) {
                    scored.push(ScoredSuggestion {
                        name: short_name.clone(),
                        item_type: ItemType::Column,
                        score,
                    });
                }

                if let Some(tbl) = table {
                    let qualified_name = format!("{}.{}", tbl, column);

                    if qualified_name.to_lowercase().starts_with(&partial_lower) &&
                       (tables_in_line.is_empty() || partial.contains('.')) {
                        scored.push(ScoredSuggestion {
                            name: qualified_name,
                            item_type: ItemType::Column,
                            score: score.saturating_sub(1),
                        });
                    }
                }
            }

            let functions = self.cache.get_functions(partial);
            for function in functions {
                if function.to_lowercase().starts_with(&partial_lower) {
                    let base_score = 1000u32;
                    let usage_count = self.usage_tracker.get_count(ItemType::Function, &function);
                    let usage_bonus = usage_count.min(99) * 10;
                    let score = base_score + usage_bonus;
                    let function_with_paren = format!("{}(", function);

                    scored.push(ScoredSuggestion {
                        name: function_with_paren,
                        item_type: ItemType::Column,
                        score,
                    });
                }
            }
        }

        // Sort by score descending
        scored.sort_by(|a, b| b.score.cmp(&a.score));

        // Deduplicate (keep highest-scored occurrence), then convert to CompletionItem
        let mut seen = std::collections::HashSet::new();
        let candidates: Vec<CompletionItem> = scored
            .into_iter()
            .filter(|s| seen.insert(s.name.clone()))
            .map(|s| {
                let description = match s.item_type {
                    ItemType::Table => "table",
                    ItemType::Column => "column",
                    ItemType::Function => "function",
                }
                .to_string();
                CompletionItem {
                    value: s.name,
                    description,
                    item_type: s.item_type,
                }
            })
            .collect();

        (word_start, candidates)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_completer_disabled() {
        let cache = Arc::new(SchemaCache::new(300));
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, false);

        let (_, candidates) = completer.complete_at("SELECT * FROM us", 17);
        assert_eq!(candidates.len(), 0);
    }

    #[test]
    fn test_completer_no_keywords() {
        let cache = Arc::new(SchemaCache::new(300));
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        let (_, candidates) = completer.complete_at("SEL", 3);

        // Should not return keywords (only tables and columns from schema cache)
        assert!(!candidates.iter().any(|c| c.value == "SELECT"));
    }
}
