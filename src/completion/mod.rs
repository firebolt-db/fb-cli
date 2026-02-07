pub mod context_analyzer;
pub mod context_detector;
pub mod priority_scorer;
pub mod schema_cache;
pub mod usage_tracker;

use context_analyzer::ContextAnalyzer;
use context_detector::find_word_start;
use priority_scorer::{PriorityScorer, ScoredSuggestion};
use rustyline::completion::{Completer, Pair};
use rustyline::Context as RustylineContext;
use schema_cache::SchemaCache;
use usage_tracker::{ItemType, UsageTracker};
use std::sync::Arc;

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

        // Extract context: tables mentioned in current statement
        let tables_in_line = ContextAnalyzer::extract_tables(line);

        // Generate scored suggestions
        let mut scored: Vec<ScoredSuggestion> = Vec::new();

        // Check if user is typing "schema_name." to see tables from that schema
        let (schema_filter, table_prefix) = if let Some(dot_pos) = partial.rfind('.') {
            // User typed something like "information_schema." or "information_schema.engine"
            let schema_part = &partial[..dot_pos];
            let table_part = &partial[dot_pos + 1..];
            (Some(schema_part), table_part)
        } else {
            (None, partial)
        };

        // If user specified a schema (e.g., "information_schema."), show tables from that schema
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
            // Normal completion: get tables and schemas
            let table_metadata = self.cache.get_tables_with_schema(partial);
            let column_metadata = self.cache.get_columns_with_table(partial);
            let schemas = self.cache.get_schemas(partial);

            // Add schema suggestions (with trailing dot)
            let partial_lower = partial.to_lowercase();

            // Check if partial matches any schema name (without dot)
            let partial_matches_schema = !partial.contains('.') &&
                schemas.iter().any(|s| s.to_lowercase().starts_with(&partial_lower));

            for schema in &schemas {
                let schema_with_dot = format!("{}.", schema);

                // Only add if it matches the partial
                if schema_with_dot.to_lowercase().starts_with(&partial_lower) {
                    // Give schemas fixed priority class 4000 (same as tables)
                    // Don't use scorer to avoid system schema deprioritization
                    let base_score = 4000u32;

                    // Add small usage bonus if schema has been used
                    let usage_count = self.usage_tracker.get_count(ItemType::Table, &schema);
                    let usage_bonus = usage_count.min(99) * 10;

                    let score = base_score + usage_bonus;

                    scored.push(ScoredSuggestion {
                        name: schema_with_dot,
                        item_type: ItemType::Table,
                        score,
                    });
                }
            }

            // Add table suggestions (both short and qualified names)
            for (schema, table) in table_metadata {
                let short_name = table.clone();
                let qualified_name = format!("{}.{}", schema, table);

                // Score the short name
                let score = self.scorer.score(&short_name, ItemType::Table, &tables_in_line, None);

                // Only add short name if it matches the partial
                if short_name.to_lowercase().starts_with(&partial_lower) {
                    scored.push(ScoredSuggestion {
                        name: short_name.clone(),
                        item_type: ItemType::Table,
                        score,
                    });
                }

                // Add qualified name only if:
                // 1. It matches the partial
                // 2. Schema is not "public" or user typed a dot
                // 3. Partial doesn't match a schema name (to avoid "infor" -> "information_schema.table")
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

            // Add column suggestions (both short and table-qualified names)
            for (table, column) in column_metadata {
                let short_name = column.clone();

                // If user typed a dot, check if they're qualifying a table name
                let is_qualifying_table = partial.contains('.') &&
                    table.as_ref().map_or(false, |t| partial.to_lowercase().starts_with(&format!("{}.", t.to_lowercase())));

                // Score the short name
                let score = self.scorer.score(
                    &short_name,
                    ItemType::Column,
                    &tables_in_line,
                    table.as_deref(),
                );

                // Only add short name if it matches the partial and user is NOT qualifying a table
                if !is_qualifying_table && short_name.to_lowercase().starts_with(&partial_lower) {
                    scored.push(ScoredSuggestion {
                        name: short_name.clone(),
                        item_type: ItemType::Column,
                        score,
                    });
                }

                // Add qualified name (table.column) if we know the table
                if let Some(tbl) = table {
                    let qualified_name = format!("{}.{}", tbl, column);

                    // Add qualified name if it matches partial and (no table context yet or user typed a dot)
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
        } // end of else block

        // Sort by score (descending - higher scores first)
        scored.sort_by(|a, b| b.score.cmp(&a.score));

        // Remove duplicates (keep first occurrence, which has highest score)
        let mut seen = std::collections::HashSet::new();
        let deduplicated: Vec<ScoredSuggestion> = scored
            .into_iter()
            .filter(|s| seen.insert(s.name.clone()))
            .collect();

        // Convert to Pair format (display, replacement)
        let pairs: Vec<Pair> = deduplicated
            .into_iter()
            .map(|s| Pair {
                display: s.name.clone(),
                replacement: s.name,
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
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, false);

        let history = DefaultHistory::new();
        let ctx = RustylineContext::new(&history);
        let result = completer.complete("SELECT * FROM us", 17, &ctx).unwrap();

        assert_eq!(result.1.len(), 0);
    }

    #[test]
    fn test_completer_no_keywords() {
        let cache = Arc::new(SchemaCache::new(300));
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        let history = DefaultHistory::new();
        let ctx = RustylineContext::new(&history);
        let result = completer.complete("SEL", 3, &ctx).unwrap();

        // Should not return keywords (only tables and columns)
        assert!(!result.1.iter().any(|p| p.display == "SELECT"));
    }
}
