pub mod candidates;
pub mod context_analyzer;
pub mod context_detector;
pub mod fuzzy_completer;
pub mod priority_scorer;
pub mod schema_cache;
pub mod usage_tracker;

use candidates::{collect_candidates, schema_in_query, table_in_query};
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

        // ── Dot-mode: partial already contains a dot ──────────────────────────
        //
        // Two sub-cases:
        //   (a) "table."     → user explicitly requests columns of that table
        //   (b) "schema.tab" → user navigates into a schema to pick a table
        //
        // We detect (a) by checking whether the part before the dot is a table
        // referenced in the current SQL. Otherwise we fall through to (b).
        if let Some(dot_pos) = partial.rfind('.') {
            let schema_part = &partial[..dot_pos];
            let table_part = &partial[dot_pos + 1..];

            let schema_lower = schema_part.to_lowercase();
            let col_prefix_lower = table_part.to_lowercase();

            // Check if the part before the dot is a query-referenced table
            let is_query_table = tables_in_line.iter().any(|t| {
                let t_lower = t.to_lowercase();
                t_lower == schema_lower
                    || t_lower.ends_with(&format!(".{}", schema_lower))
                    || schema_lower.ends_with(&format!(".{}", t_lower))
            });

            let mut scored: Vec<ScoredSuggestion> = Vec::new();

            if is_query_table {
                // (a) Emit columns of this table whose name starts with the column prefix
                let column_metadata = self.cache.get_columns_with_table(partial);
                for (table, column) in column_metadata {
                    if let Some(tbl) = table {
                        if tbl.to_lowercase() == schema_lower
                            && column.to_lowercase().starts_with(&col_prefix_lower)
                        {
                            let qualified_name = format!("{}.{}", tbl, column);
                            let score = self.scorer.score(
                                &qualified_name,
                                ItemType::Column,
                                &tables_in_line,
                                Some(&tbl),
                            );
                            scored.push(ScoredSuggestion {
                                name: qualified_name,
                                item_type: ItemType::Column,
                                score,
                            });
                        }
                    }
                }
            } else {
                // (b) Emit tables within the named schema
                let tables_in_schema =
                    self.cache.get_tables_in_schema(schema_part, table_part);
                for table in tables_in_schema {
                    let qualified_name = format!("{}.{}", schema_part, table);
                    let score =
                        self.scorer.score(&table, ItemType::Table, &tables_in_line, None);
                    scored.push(ScoredSuggestion {
                        name: qualified_name,
                        item_type: ItemType::Table,
                        score,
                    });
                }
            }

            scored.sort_by(|a, b| b.score.cmp(&a.score).then(a.name.cmp(&b.name)));
            let mut seen = std::collections::HashSet::new();
            let items: Vec<CompletionItem> = scored
                .into_iter()
                .filter(|s| seen.insert(s.name.clone()))
                .map(|s| CompletionItem {
                    value: s.name,
                    description: match s.item_type {
                        ItemType::Column => "column",
                        ItemType::Table => "table",
                        ItemType::Function => "function",
                        ItemType::Schema => "schema",
                    }
                    .to_string(),
                    item_type: s.item_type,
                })
                .collect();

            return (word_start, items);
        }

        // ── No-dot mode: unified candidate collection + tab selectivity ────────
        //
        // Collect all schema items with context-aware priority, then apply
        // tab-completion's extra selectivity rules:
        //   • Columns   — only if their table appears in the current SQL.
        //   • Non-public tables — only if their schema appears in the current
        //     SQL *or* the user is explicitly typing the schema name as a prefix.
        //   • Schema completions (e.g. "information_schema.") — only when the
        //     partial does not already contain a dot (suppressed above).

        let all = collect_candidates(&self.cache, self.usage_tracker.clone(), &tables_in_line);
        let partial_lower = partial.to_lowercase();

        let mut filtered: Vec<&candidates::Candidate> = all
            .iter()
            // ── prefix match ──────────────────────────────────────────────────
            .filter(|c| c.prefix_matches(partial))
            // ── tab-specific selectivity ──────────────────────────────────────
            .filter(|c| match c.item_type {
                ItemType::Column => {
                    // Show only columns whose table appears in the current SQL.
                    let table = c.table_name.as_deref().unwrap_or("");
                    table_in_query(table, &c.schema, &tables_in_line)
                }
                ItemType::Table => {
                    // Public tables: always visible.
                    // Non-public tables:
                    //   • non-empty partial → always show when prefix matches
                    //     (user is deliberately typing toward this table)
                    //   • empty partial → only show if the schema is already
                    //     referenced in the current SQL (avoids flooding the
                    //     list with dozens of system-schema tables on Tab alone)
                    c.schema == "public"
                        || c.schema.is_empty()
                        || !partial.is_empty()
                        || schema_in_query(&c.schema, &tables_in_line)
                }
                ItemType::Schema => {
                    // Show schema completions only when:
                    // (a) no dot has been typed yet (dot-mode handles the rest), AND
                    // (b) the user is explicitly typing the schema name prefix, OR
                    //     the schema is already referenced somewhere in the SQL.
                    !partial_lower.contains('.')
                        && (!partial.is_empty()
                            || schema_in_query(&c.schema, &tables_in_line))
                }
                ItemType::Function => true,
            })
            .collect();

        // Sort by priority descending, then alphabetically as tiebreaker
        filtered.sort_by(|a, b| {
            b.priority
                .cmp(&a.priority)
                .then(a.display.cmp(&b.display))
        });

        // Deduplicate (keep highest-priority occurrence)
        let mut seen = std::collections::HashSet::new();
        let items: Vec<CompletionItem> = filtered
            .into_iter()
            .filter(|c| seen.insert(c.display.clone()))
            .map(|c| CompletionItem {
                value: c.insert.clone(),
                description: c.description.to_string(),
                item_type: c.item_type,
            })
            .collect();

        (word_start, items)
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

    // ── Tab selectivity: columns ──────────────────────────────────────────────

    /// Columns from tables NOT in the current SQL must be hidden.
    #[test]
    fn test_tab_hides_columns_from_unrelated_tables() {
        use schema_cache::{ColumnMetadata, TableMetadata};

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![
            TableMetadata {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![ColumnMetadata {
                    name: "order_id".to_string(),
                    data_type: "integer".to_string(),
                }],
            },
            TableMetadata {
                schema_name: "public".to_string(),
                table_name: "users".to_string(),
                columns: vec![ColumnMetadata {
                    name: "user_id".to_string(),
                    data_type: "integer".to_string(),
                }],
            },
        ]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // SQL references "orders" but NOT "users"
        let (_, cands) = completer.complete_at("SELECT ord FROM orders", 10);

        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(
            values.iter().any(|v| v.contains("order_id")),
            "should suggest orders.order_id"
        );
        assert!(
            !values.iter().any(|v| v.contains("user_id")),
            "should NOT suggest user_id (users table not in SQL)"
        );
    }

    /// Columns from the referenced table should be suggested.
    #[test]
    fn test_tab_shows_columns_for_referenced_table() {
        use schema_cache::{ColumnMetadata, TableMetadata};

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![TableMetadata {
            schema_name: "public".to_string(),
            table_name: "users".to_string(),
            columns: vec![
                ColumnMetadata {
                    name: "user_id".to_string(),
                    data_type: "integer".to_string(),
                },
                ColumnMetadata {
                    name: "username".to_string(),
                    data_type: "text".to_string(),
                },
            ],
        }]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // Empty partial after the table reference
        let (_, cands) = completer.complete_at("SELECT  FROM users", 7);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(values.iter().any(|v| v.contains("user_id")));
        assert!(values.iter().any(|v| v.contains("username")));
    }

    // ── Tab selectivity: non-public tables ────────────────────────────────────

    /// Non-public tables must be hidden when their schema is not in the SQL.
    #[test]
    fn test_tab_hides_nonpublic_table_when_schema_not_in_sql() {
        use schema_cache::TableMetadata;

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![
            TableMetadata {
                schema_name: "public".to_string(),
                table_name: "orders".to_string(),
                columns: vec![],
            },
            TableMetadata {
                schema_name: "analytics".to_string(),
                table_name: "events".to_string(),
                columns: vec![],
            },
        ]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // SQL does not mention "analytics" schema
        let (_, cands) = completer.complete_at("SELECT  FROM orders", 7);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(
            !values.iter().any(|v| v.contains("analytics")),
            "should NOT show analytics.events when analytics is not in SQL"
        );
        assert!(
            values.iter().any(|v| *v == "orders"),
            "should still show public table"
        );
    }

    /// Non-public tables must be visible when their schema appears in the SQL.
    #[test]
    fn test_tab_shows_nonpublic_table_when_schema_in_sql() {
        use schema_cache::TableMetadata;

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![TableMetadata {
            schema_name: "analytics".to_string(),
            table_name: "events".to_string(),
            columns: vec![],
        }]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // SQL already references analytics.events
        let (_, cands) =
            completer.complete_at("SELECT  FROM analytics.events", 7);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(
            values.iter().any(|v| v.contains("analytics.events")),
            "should show analytics.events when analytics is referenced"
        );
    }

    /// Non-public table must show when the user explicitly types the schema prefix.
    #[test]
    fn test_tab_shows_nonpublic_table_when_typing_schema_prefix() {
        use schema_cache::TableMetadata;

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![TableMetadata {
            schema_name: "analytics".to_string(),
            table_name: "events".to_string(),
            columns: vec![],
        }]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // User typed "analytics" (the schema name) in the no-schema-in-sql case
        let (_, cands) = completer.complete_at("SELECT analytics FROM", 16);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(
            values.iter().any(|v| v.contains("analytics")),
            "should show analytics.events when user types analytics prefix"
        );
    }

    // ── Dot-mode ──────────────────────────────────────────────────────────────

    /// "table." should produce columns from that table (when table is in SQL).
    #[test]
    fn test_dot_mode_shows_columns_when_table_in_sql() {
        use schema_cache::{ColumnMetadata, TableMetadata};

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![TableMetadata {
            schema_name: "public".to_string(),
            table_name: "test".to_string(),
            columns: vec![
                ColumnMetadata {
                    name: "val".to_string(),
                    data_type: "integer".to_string(),
                },
                ColumnMetadata {
                    name: "name".to_string(),
                    data_type: "text".to_string(),
                },
            ],
        }]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        let (_, cands) = completer.complete_at("SELECT test. FROM test", 12);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(values.iter().any(|v| v.contains("val")));
        assert!(values.iter().any(|v| v.contains("name")));
    }

    /// Column filter should also work when the table is schema-qualified in SQL.
    #[test]
    fn test_tab_columns_schema_qualified_in_sql() {
        use schema_cache::{ColumnMetadata, TableMetadata};

        let cache = Arc::new(SchemaCache::new(300));
        cache.inject_test_tables(vec![TableMetadata {
            schema_name: "information_schema".to_string(),
            table_name: "engine_query_history".to_string(),
            columns: vec![ColumnMetadata {
                name: "start_time".to_string(),
                data_type: "timestamptz".to_string(),
            }],
        }]);
        let usage_tracker = Arc::new(UsageTracker::new(10));
        let completer = SqlCompleter::new(cache, usage_tracker, true);

        // Table referenced as "information_schema.engine_query_history"
        let (_, cands) = completer
            .complete_at("SELECT  FROM information_schema.engine_query_history", 7);
        let values: Vec<&str> = cands.iter().map(|c| c.value.as_str()).collect();
        assert!(
            values.iter().any(|v| v.contains("start_time")),
            "should suggest start_time for information_schema.engine_query_history"
        );
    }
}
